import type { Metadata } from "next";
import { Inter } from "next/font/google";
import { Analytics } from "@vercel/analytics/next";
import "./globals.css";
import Header from "@/components/Header";
import Footer from "@/components/Footer";
import Providers from "@/components/Providers";

const inter = Inter({
  subsets: ["latin"],
});

export const metadata: Metadata = {
  metadataBase: new URL(process.env.NEXT_PUBLIC_SITE_URL || "https://theaggregate.xyz"),
  title: "The Aggregate | Multi-Source Consensus NBA Picks",
  description:
    "Daily NBA picks scored against 8 historical dimensions. Aggregated from 7+ expert sources with full transparency.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body className={`${inter.className} bg-gray-950 text-gray-100 min-h-screen`}>
        <Providers>
          <Header />
          <main>{children}</main>
          <Footer />
          <Analytics />
        </Providers>
      </body>
    </html>
  );
}
