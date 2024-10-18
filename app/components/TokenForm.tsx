"use client"

import { useState } from 'react'
import { useForm } from 'react-hook-form'
import { Button } from "@/components/ui/button"
import { Form, FormControl, FormField, FormItem, FormLabel, FormMessage } from "@/components/ui/form"
import { Input } from "@/components/ui/input"

export function TokenForm() {
  const [message, setMessage] = useState('')
  const [isProcessing, setIsProcessing] = useState(false)
  const [downloadLink, setDownloadLink] = useState('')

  const form = useForm({
    defaultValues: {
      token: "",
    },
  })

  async function onSubmit(values: { token: string }) {
    if (isProcessing) return; // Prevent multiple submissions
    setIsProcessing(true)
    setMessage('') // Set message to indicate processing state
    setDownloadLink('')

    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/validate-and-process`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ token: values.token }),
      })
      const data = await response.json()
      
      if (data.valid) {
        setMessage('Token is valid. Data processed successfully.')
        setDownloadLink(data.download_link)
      } else {
        setMessage('Token is invalid or processing failed.')
      }
    } catch (error) {
      setMessage('An error occurred while processing the request.')
    } finally {
      setIsProcessing(false)
    }
  }

  return (
    <Form {...form}>
      <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-8">
        <FormField
          control={form.control}
          name="token"
          render={({ field }) => (
            <FormItem>
              <FormLabel>API Token</FormLabel>
              <FormControl>
                <Input placeholder="Enter your API token" {...field} />
              </FormControl>
              <FormMessage />
            </FormItem>
          )}
        />
        {message && <p>{message}</p>}
        {!downloadLink ? (
          <Button className='mt-4' type="submit" disabled={isProcessing}>
            {isProcessing ? 'Processing...' : 'Submit'}
          </Button>
        ) : (
          <a href={`${process.env.NEXT_PUBLIC_API_URL}${downloadLink.replace(/^\/api/, '')}`} download>
            <Button className='mt-4' type="button">
              Download Processed Data
            </Button>
          </a>
        )}
      </form>
    </Form>
  )
}