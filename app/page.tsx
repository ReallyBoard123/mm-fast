import { TokenForm } from "@/components/TokenForm"

export default function Home() {
  return (
    <div className="container mx-auto p-4">
      <div className="mb-8">
        <h2 className="text-xl font-semibold mb-2">Process with Token</h2>
        <TokenForm />
      </div>
    </div>
  )
}