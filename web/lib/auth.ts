import "server-only";

import type { NextAuthOptions } from "next-auth";

const clientId = process.env.WHOP_CLIENT_ID || "";
const clientSecret = process.env.WHOP_CLIENT_SECRET || "";

/** Whether Whop auth is configured (credentials present) */
export const isAuthEnabled = Boolean(clientId);

export const authOptions: NextAuthOptions = {
  providers: isAuthEnabled
    ? [
        {
          id: "whop",
          name: "Whop",
          type: "oauth" as const,
          wellKnown:
            "https://api.whop.com/.well-known/openid-configuration",
          clientId,
          clientSecret,
          client: {
            token_endpoint_auth_method: "client_secret_post",
            id_token_signed_response_alg: "ES256",
          },
          authorization: {
            params: { scope: "openid profile email" },
          },
          idToken: true,
          checks: ["pkce", "state", "nonce"],
          profile(profile: {
            sub: string;
            name?: string;
            email?: string;
            picture?: string;
          }) {
            return {
              id: profile.sub,
              name: profile.name || profile.sub,
              email: profile.email || null,
              image: profile.picture || null,
            };
          },
        },
      ]
    : [],
  callbacks: {
    async jwt({ token, account, user }) {
      if (user) {
        token.whopUserId = user.id;
      }
      if (account) {
        token.accessToken = account.access_token;
      }
      return token;
    },
    async session({ session, token }) {
      if (token.whopUserId) {
        (session as any).whopUserId = token.whopUserId as string;
      }
      return session;
    },
  },
  secret: process.env.NEXTAUTH_SECRET,
  // Required for Vercel: trust the x-forwarded-proto header so NextAuth
  // knows it's running over HTTPS and sets secure cookies correctly.
  // Without this, cookie mismatch causes ERR_TOO_MANY_REDIRECTS.
  cookies: {
    sessionToken: {
      name: `__Secure-next-auth.session-token`,
      options: {
        httpOnly: true,
        sameSite: "lax",
        path: "/",
        secure: true,
      },
    },
    callbackUrl: {
      name: `__Secure-next-auth.callback-url`,
      options: {
        httpOnly: false,
        sameSite: "lax",
        path: "/",
        secure: true,
      },
    },
    csrfToken: {
      name: `__Host-next-auth.csrf-token`,
      options: {
        httpOnly: true,
        sameSite: "lax",
        path: "/",
        secure: true,
      },
    },
  },
};
