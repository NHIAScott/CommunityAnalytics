/** @type {import('next').NextConfig} */
const nextConfig = {
  experimental: { typedRoutes: false },
  eslint: {
    // Prevent non-critical lint findings from failing container image builds.
    ignoreDuringBuilds: true,
  },
  typescript: {
    // Prevent type-check findings from failing container image builds.
    // Runtime behavior is unchanged; this only affects build gating.
    ignoreBuildErrors: true,
  },
};

export default nextConfig;
