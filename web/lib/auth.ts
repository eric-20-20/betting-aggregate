import "server-only";

import type { NextAuthOptions } from "next-auth";

const clientId = process.env.WHOP_CLIENT_ID || "";
const clientSecret = process.env.WHOP_CLIENT_SECRET || "";

/** Whether Whop auth is configured (credentials present) */
export const isAuthEnabled = Boolean(clientId && clientSecret);

export const authOptions: NextAuthOptions = {
  providers: isAuthEnabled
    ? [
        {
          id: "whop",
          name: "Whop",
          type: "oauth",
          clientId,
          clientSecret,
          authorization: {
            url: "https://api.whop.com/oauth/authorize",
            params: { scope: "openid offline" },
          },
          token: "https://data.whop.com/api/v3/oauth/token",
          userinfo: {
            url: "https://api.whop.com/api/v5/me",
            async request({ tokens }) {
              const res = await fetch("https://api.whop.com/api/v5/me", {
                headers: { Authorization: `Bearer ${tokens.access_token}` },
              });
              if (!res.ok) throw new Error("Failed to fetch Whop user");
              return res.json();
            },
          },
          checks: ["pkce", "state"],
          profile(profile) {
            return {
              id: profile.id,
              name: profile.username || profile.name || profile.id,
              email: profile.email || null,
              image: profile.profile_pic_url || null,
            };
          },
        },
      ]
    : [],
  callbacks: {
    async jwt({ token, account, profile }) {
      // On initial sign-in, persist Whop-specific fields
      if (account && profile) {
        token.whopUserId = (profile as { id: string }).id;
        token.accessToken = account.access_token;
      }
      return token;
    },
    async session({ session, token }) {
      // Expose whopUserId to the session so server components can use it
      if (token.whopUserId) {
        (session as any).whopUserId = token.whopUserId as string;
      }
      return session;
    },
  },
  pages: {
    signIn: "/api/auth/signin",
  },
  secret: process.env.NEXTAUTH_SECRET,
};
