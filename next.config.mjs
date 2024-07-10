/** @type {import('next').NextConfig} */
const nextConfig = {
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: process.env.NODE_ENV === "development"
          ? "http://127.0.0.1:8000/api/:path*"
          : "https://your-backend-url.onrender.com/api/:path*",  // Update this with your Render backend URL
      },
      
    ];
  },
};

export default nextConfig;