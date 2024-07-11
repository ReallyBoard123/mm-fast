"use client"

import { zodResolver } from "@hookform/resolvers/zod"
import { useForm } from "react-hook-form"
import { z } from "zod"
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

import { useState } from "react"
import { LogDrawer } from "./LogDrawer"

// Define the schema using Zod
const formSchema = z.object({
  token: z.string().min(10, {
    message: "Token must be at least 10 characters.",
  }),
  save_folder: z.string().min(1, {
    message: "Save folder path is required.",
  }),
})

export function APIForm() {
  const [logs, setLogs] = useState<string[]>([])
  const [loading, setLoading] = useState(false)
  const form = useForm<z.infer<typeof formSchema>>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      token: "",
      save_folder: "",
    },
  })

  async function onSubmit(values: z.infer<typeof formSchema>) {
    setLoading(true)
    setLogs([]) // Clear previous logs
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

      setLogs(data.logs)
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
    <>
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
          <FormField
            control={form.control}
            name="save_folder"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Save Folder</FormLabel>
                <FormControl>
                  <Input placeholder="Enter the save folder path" {...field} />
                </FormControl>
                <FormDescription>
                  Provide the absolute path to the folder where you want to save the data.
                </FormDescription>
                <FormMessage />
              </FormItem>
            )}
          />
          <div className="flex space-x-4">
            <Button type="submit" disabled={loading}>
              {loading ? "Submitting..." : "Submit"}
            </Button>
            <LogDrawer logs={logs} />
          </div>
        </form>  
      </Form>
    </>
  )
}
