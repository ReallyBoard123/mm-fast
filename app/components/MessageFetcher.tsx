"use client"


import React, { useState } from 'react';
import { Button } from "@/components/ui/button";

const MessageFetcher: React.FC = () => {
  const [message, setMessage] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState("");

  const fetchMessage = async () => {
    setIsLoading(true);
    setError("");
    try {
      const apiUrl = '/api/python';
      const response = await fetch(apiUrl);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      setMessage(data.message);
    } catch (error) {
      console.error("Error fetching message:", error);
      setError(`Error fetching message: ${(error as Error).message}`);
      setMessage("");
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
      {error && (
        <div className="p-4 bg-red-100 text-red-700 rounded-md">
          <p>{error}</p>
        </div>
      )}
    </div>
  );
}

export default MessageFetcher;