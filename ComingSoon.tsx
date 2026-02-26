import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"

export default function ComingSoonPage() {
  return (
    <div className="min-h-[calc(100vh-4rem)] w-full flex items-center justify-center px-4 py-10">
      <Card className="w-full max-w-xl">
        <CardHeader>
          <div className="flex items-center justify-between gap-3">
            <CardTitle className="text-2xl">Coming soon</CardTitle>
            <Badge variant="secondary">In development</Badge>
          </div>
          <CardDescription>
            This feature is being built and will be available in a future release.
          </CardDescription>
        </CardHeader>

        <CardContent className="space-y-4">
          <div className="space-y-2 text-sm text-muted-foreground">
            <p>
              We’re working on a new experience to make this area more powerful and easier to use.
            </p>
            <ul className="list-disc pl-5 space-y-1">
              <li>Clearer workflows and guidance</li>
              <li>Better performance and reliability</li>
              <li>More visibility into status and outcomes</li>
            </ul>
          </div>

          <div className="flex flex-wrap gap-2 pt-2">
            <Button asChild>
              <a href="/">Back to home</a>
            </Button>
            <Button variant="outline" asChild>
              <a href="/changelog">View updates</a>
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
