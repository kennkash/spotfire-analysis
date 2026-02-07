import * as React from "react"
import { ChevronDown } from "lucide-react"

import { ModeToggle } from "@/components/mode-toggle"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Separator } from "@/components/ui/separator"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"

type Row = {
  metric: string
  current: number | string
  target: number | string
  savings: string
}

const TEAMS = ["Factory Ops", "Data Platform", "Analytics", "Process Engineering"] as const

const MOCK_DATA: Record<(typeof TEAMS)[number], Row[]> = {
  "Factory Ops": [
    { metric: "Analyst licenses", current: 120, target: 70, savings: "$48,000" },
    { metric: "Consumer licenses", current: 300, target: 240, savings: "$12,000" },
    { metric: "Monthly active users", current: 410, target: 410, savings: "$0" },
  ],
  "Data Platform": [
    { metric: "Analyst licenses", current: 80, target: 45, savings: "$28,000" },
    { metric: "Business Author", current: 40, target: 30, savings: "$10,000" },
    { metric: "Orphaned seats", current: 12, target: 0, savings: "$6,000" },
  ],
  Analytics: [
    { metric: "Analyst licenses", current: 200, target: 120, savings: "$64,000" },
    { metric: "Consumers converted", current: 35, target: 60, savings: "$18,000" },
    { metric: "Unused last 90d", current: 22, target: 0, savings: "$11,000" },
  ],
  "Process Engineering": [
    { metric: "Analyst licenses", current: 60, target: 35, savings: "$20,000" },
    { metric: "Consumers", current: 110, target: 95, savings: "$6,000" },
    { metric: "Inactive > 60d", current: 14, target: 0, savings: "$7,000" },
  ],
}

function SidebarItem({
  active,
  onClick,
  children,
}: {
  active: boolean
  onClick: () => void
  children: React.ReactNode
}) {
  return (
    <Button
      variant={active ? "secondary" : "ghost"}
      className="w-full justify-start"
      onClick={onClick}
    >
      {children}
    </Button>
  )
}

export default function App() {
  const [activeMenu, setActiveMenu] = React.useState<"license" | "other">("license")
  const [licenseExpanded, setLicenseExpanded] = React.useState(true)
  const [team, setTeam] = React.useState<string>("")

  const rows = React.useMemo<Row[]>(() => {
    if (!team) return []
    return (MOCK_DATA as Record<string, Row[]>)[team] ?? []
  }, [team])

  return (
    <div className="min-h-screen bg-background text-foreground flex flex-col">
      {/* HEADER (always visible) */}
      <header className="sticky top-0 z-50 border-b bg-background/80 backdrop-blur">
        <div className="h-16 px-4 flex items-center justify-between">
          <div className="flex items-baseline gap-3">
            <div className="text-lg font-semibold">Spotfire License Hub</div>
          </div>
          <ModeToggle />
        </div>
      </header>

      {/* CONTENT (sidebar + main) */}
      <div className="flex flex-1 min-h-0">
        {/* SIDEBAR */}
        <aside className="w-72 border-r p-4 flex flex-col gap-3 overflow-y-auto">
          <div>
            <div className="text-sm font-semibold">Navigation</div>
            <div className="text-xs text-muted-foreground">Choose a view</div>
          </div>

          <Separator />

          {/* License reduction section w/ dropdown */}
          <div className="space-y-1">
            <div className="flex items-center justify-between">
              <SidebarItem
                active={activeMenu === "license"}
                onClick={() => {
                  setActiveMenu("license")
                  setLicenseExpanded((v) => !v)
                }}
              >
                <span className="flex-1 text-left">License reduction data</span>
                <ChevronDown
                  className={[
                    "h-4 w-4 transition-transform",
                    licenseExpanded ? "rotate-0" : "-rotate-90",
                  ].join(" ")}
                />
              </SidebarItem>
            </div>

            {activeMenu === "license" && licenseExpanded && (
              <div className="pl-2 pt-2 space-y-2">
                <div className="text-xs text-muted-foreground">Team</div>
                <Select value={team} onValueChange={setTeam}>
                  <SelectTrigger className="w-full">
                    <SelectValue placeholder="Select a team..." />
                  </SelectTrigger>
                  <SelectContent>
                    {TEAMS.map((t) => (
                      <SelectItem key={t} value={t}>
                        {t}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}
          </div>

          <Separator />

          <SidebarItem active={activeMenu === "other"} onClick={() => setActiveMenu("other")}>
            Other menu item
          </SidebarItem>

          <div className="mt-auto pt-4">
            <Button className="w-full" onClick={() => alert("Settings placeholder")}>
              Settings
            </Button>
          </div>
        </aside>

        {/* MAIN */}
        <main className="flex-1 p-6 overflow-y-auto">
          {activeMenu === "license" ? (
            <div className="space-y-4">
              <div>
                <h1 className="text-2xl font-semibold">License reduction data</h1>
                <p className="text-sm text-muted-foreground">
                  Select a team in the sidebar to load their license posture.
                </p>
              </div>

              <Card>
                <CardHeader>
                  <CardTitle>
                    {team ? `Team: ${team}` : "No team selected"}
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {!team ? (
                    <div className="text-sm text-muted-foreground">
                      Use the sidebar dropdown to select a team.
                    </div>
                  ) : (
                    <div className="rounded-md border">
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead>Metric</TableHead>
                            <TableHead>Current</TableHead>
                            <TableHead>Target</TableHead>
                            <TableHead>Estimated savings</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {rows.map((r) => (
                            <TableRow key={r.metric}>
                              <TableCell className="font-medium">{r.metric}</TableCell>
                              <TableCell>{r.current}</TableCell>
                              <TableCell>{r.target}</TableCell>
                              <TableCell>{r.savings}</TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>
          ) : (
            <Card>
              <CardHeader>
                <CardTitle>Other menu item</CardTitle>
              </CardHeader>
              <CardContent className="text-sm text-muted-foreground">
                Placeholder page content.
              </CardContent>
            </Card>
          )}
        </main>
      </div>

      {/* FOOTER (always visible) */}
      <footer className="sticky bottom-0 border-t bg-background/80 backdrop-blur">
        <div className="h-10 px-4 flex items-center justify-center text-xs text-muted-foreground">
          Digital Solutions - KS
        </div>
      </footer>
    </div>
  )
}
