"use client"

import React, { useState } from 'react';
import axios from 'axios';
import { Button } from "../components/ui/button";

const MessageFetcher: React.FC = () => {
  const [message, setMessage] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState("");

  const fetchMessage = async () => {
    setIsLoading(true);
    setError("");
    try {
      const response = await axios.get(`${process.env.NEXT_PUBLIC_API_URL}/python`);
      const data = response.data;
      setMessage(data.message);
    } catch (error) {
      console.error("Error fetching message:", error);
      if (axios.isAxiosError(error)) {
        setError(`Error fetching message: ${(error as Error).message}`);
      } else {
        setError("An unknown error occurred");
      }
      setMessage("");
    } finally {
      setIsLoading(false);
    }
  };
  

  return (
    <div className="flex flex-col items-center space-y-4">
      <Button onClick={fetchMessage} disabled={isLoading}>
        {isLoading ? "Loading..." : "Check Server Connection"}
      </Button>
      {message && (
        <div className="p-2 bg-gray-100 rounded-md">
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
