import { APIForm } from "./components/APIForm";
import MessageFetcher from "./components/MessageFetcher";

export default function Home() {
  return (
    <main className="flex min-h-screen flex-col items-center justify-center p-8 space-y-4">
      <MessageFetcher />
      <APIForm />
    </main>
  );
}
