"use client"

import { useState } from "react"
import { useForm } from "react-hook-form"
import { z } from "zod"
import { zodResolver } from "@hookform/resolvers/zod"
import { Button } from "../components/ui/button"
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
    setLoading(true)
    setLogs([])
    setDownloadLink(null)
    try {
      const response = await fetch("/api/process", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(values),
      })

      const data = await response.json()
      if (!response.ok) {
        throw new Error(data.detail || "Something went wrong")
      }

      setLogs(data.logs || [])
      if (data.download_link) {
        setDownloadLink(data.download_link)
      }
    } catch (error: unknown) {
      console.error("Error:", error)
      if (error instanceof Error) {
        setLogs((prevLogs) => [...prevLogs, (error as Error).message])
      } else {
        setLogs((prevLogs) => [...prevLogs, "An unknown error occurred"])
      }
    } finally {
      setLoading(false)
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