import * as React from "react"
import { Drawer, DrawerClose, DrawerContent, DrawerDescription, DrawerFooter, DrawerHeader, DrawerTitle, DrawerTrigger } from "@/components/ui/drawer"
import { Button } from "@/components/ui/button"

interface LogDrawerProps {
  logs: string[]
}

export function LogDrawer({ logs }: LogDrawerProps) {
  const [open, setOpen] = React.useState(false)

  return (
    <Drawer open={open} onOpenChange={setOpen}>
      <DrawerTrigger asChild>
        <Button variant="outline">View Logs</Button>
      </DrawerTrigger>
      <DrawerContent>
        <DrawerHeader>
          <DrawerTitle>Download Logs</DrawerTitle>
          <DrawerDescription>You can see all the downloaded and processed files here.</DrawerDescription>
        </DrawerHeader>
        <div className="p-4 overflow-y-auto max-h-[60vh]">
          {logs.map((log, index) => (
            <div key={index} className="mb-2">
              {log}
            </div>
          ))}
        </div>
        <DrawerFooter>
          <DrawerClose asChild>
            <Button variant="outline">Close</Button>
          </DrawerClose>
        </DrawerFooter>
      </DrawerContent>
    </Drawer>
  )
}
