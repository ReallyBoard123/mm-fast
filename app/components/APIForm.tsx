"use client"

import { useState } from "react"
import { useForm } from "react-hook-form"
import { z } from "zod"
import { zodResolver } from "@hookform/resolvers/zod"
import { Button } from "../components/ui/button"
import axios from "axios";
import {
  Form,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from "../components/ui/form"
import { Input } from "../components/ui/input"
import { LogDrawer } from "./LogDrawer"

const formSchema = z.object({
  token: z.string().min(10, {
    message: "Token must be at least 10 characters.",
  }),
})

export function APIForm() {
  const [logs, setLogs] = useState<string[]>([])
  const [loading, setLoading] = useState(false)
  const [downloadLink, setDownloadLink] = useState<string | null>(null)

  const form = useForm<z.infer<typeof formSchema>>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      token: "",
    },
  })

  async function onSubmit(values: z.infer<typeof formSchema>) {
    setLoading(true);
    setLogs([]);
    setDownloadLink(null);
    try {
      const response = await axios.post("/api/process", values);
  
      if (response.status !== 200) {
        throw new Error(response.data.detail || "Something went wrong");
      }
  
      setLogs(response.data.logs || []);
      if (response.data.download_link) {
        setDownloadLink(response.data.download_link);
      }
    } catch (error) {
      console.error("Error:", error);
      if (axios.isAxiosError(error)) {
        setLogs((prevLogs) => [...prevLogs, (error as Error).message]);
      } else {
        setLogs((prevLogs) => [...prevLogs, "An unknown error occurred"]);
      }
    } finally {
      setLoading(false);
    }
  }
  

  return (
    <div className="space-y-8">
      <Form {...form}>
        <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-8">
          <FormField
            control={form.control}
            name="token"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Access Token</FormLabel>
                <FormControl>
                  <Input placeholder="Enter your access token" {...field} />
                </FormControl>
                <FormDescription>
                  Please enter the access token provided to you.
                </FormDescription>
                <FormMessage />
              </FormItem>
            )}
          />
          <div className="flex space-x-4">
            <Button type="submit" disabled={loading}>
              {loading ? "Processing..." : "Submit"}
            </Button>
            <LogDrawer logs={logs} />
          </div>
        </form>
      </Form>
      {downloadLink && (
        <div className="mt-4">
          <Button 
            onClick={() => window.open(downloadLink, '_blank')}
            disabled={loading}
          >
            Download Processed Data
          </Button>
        </div>
      )}
    </div>
  )
}
