import MessageFetcher from "./components/MessageFetcher";

export default function Home() {
  return (
    <main className="flex min-h-screen flex-col items-center justify-between p-24">
      <MessageFetcher />
    </main>
  );
}
