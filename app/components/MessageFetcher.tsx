"use client"

import { useState } from 'react';
import { Button } from "@/components/ui/button";

const MessageFetcher: React.FC = () => {
  const [message, setMessage] = useState("");
  const [isLoading, setIsLoading] = useState(false);

  const fetchMessage = async () => {
    setIsLoading(true);
    try {
      const apiUrl = '/api/python';
      const response = await fetch(apiUrl);
      if (!response.ok) {
        throw new Error("Network response was not ok");
      }
      const data = await response.json();
      setMessage(data.message);
    } catch (error) {
      console.error("Error fetching message:", error);
      setMessage("Error fetching message");
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="flex flex-col items-center space-y-4">
      <Button onClick={fetchMessage} disabled={isLoading}>
        {isLoading ? "Loading..." : "Fetch Message"}
      </Button>
      {message && (
        <div className="p-4 bg-gray-100 rounded-md">
          <p>{message}</p>
        </div>
      )}
    </div>
  );
}

export default MessageFetcher;