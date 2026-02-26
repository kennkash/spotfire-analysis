// spotfire-license-hub/src/components/license-reduction/license-reduction-view.tsx

"use client"

import * as React from "react"
import { useQuery } from "@tanstack/react-query"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"

import { getApiBase } from "@/lib/apiBase"

type LicenseRow = {
  name: string
  user: string
  email: string
  costCenterName: string
  departmentName: string
  title: string
  statusName: string
  recommendedAction: "Analyst" | "Consumer" | string
}

type SortKey = keyof Pick<
  LicenseRow,
  "name" | "user" | "email" | "costCenterName" | "departmentName" | "title" | "statusName" | "recommendedAction"
>

type SortDir = "asc" | "desc"

async function fetchCostCenters(): Promise<string[]> {
  const base = getApiBase()

  const res = await fetch(`${base}/v0/cost-centers`, {
    credentials: "include",
    headers: { "Cache-Control": "no-store" },
  })

  if (!res.ok) throw new Error(`API error ${res.status}`)
  return res.json()
}

async function fetchRows(costCenter: string): Promise<LicenseRow[]> {
  const base = getApiBase()

  const url = new URL(`${base}/v0/license-reduction`, window.location.origin)
  url.searchParams.set("cost_center_name", costCenter)

  const res = await fetch(url.toString(), {
    credentials: "include",
    headers: { "Cache-Control": "no-store" },
  })

  if (!res.ok) throw new Error(`API error ${res.status}`)
  return res.json()
}

function sortIcon(active: boolean, dir: SortDir) {
  if (!active) return ""
  return dir === "asc" ? "▲" : "▼"
}

export default function LicenseReductionView() {
  const [costCenter, setCostCenter] = React.useState("")
  const [search, setSearch] = React.useState("")
  const [sortKey, setSortKey] = React.useState<SortKey | null>(null)
  const [sortDir, setSortDir] = React.useState<SortDir>("asc")

  const { data: costCenters = [] } = useQuery({
    queryKey: ["cost-centers"],
    queryFn: fetchCostCenters,
    staleTime: 5 * 60 * 1000,
  })

  const { data: rows = [], isLoading } = useQuery({
    queryKey: ["license-reduction", costCenter],
    queryFn: () => fetchRows(costCenter),
    enabled: !!costCenter,
    refetchOnWindowFocus: false,
    staleTime: 30 * 1000,
    retry: 1,
  })

  // Reset search + sorting when switching cost centers (optional, but keeps things sane)
  React.useEffect(() => {
    setSearch("")
    setSortKey(null)
    setSortDir("asc")
  }, [costCenter])

  const analystCount = React.useMemo(
    () => rows.filter((r) => r.recommendedAction === "Analyst").length,
    [rows]
  )

  const consumerCount = React.useMemo(
    () => rows.filter((r) => r.recommendedAction === "Consumer").length,
    [rows]
  )

  // Default ordering: Analysts first, then username
  const defaultOrderedRows = React.useMemo<LicenseRow[]>(() => {
    if (!rows.length) return []

    const analysts = rows.filter((r) => r.recommendedAction === "Analyst")
    const consumers = rows.filter((r) => r.recommendedAction !== "Analyst")

    const byUsername = (a: LicenseRow, b: LicenseRow) =>
      String(a.user ?? "").localeCompare(String(b.user ?? ""), undefined, { sensitivity: "base" })

    analysts.sort(byUsername)
    consumers.sort(byUsername)

    return [...analysts, ...consumers]
  }, [rows])

  // Search filter (applies on top of default ordering)
  const filteredRows = React.useMemo<LicenseRow[]>(() => {
    const q = search.trim().toLowerCase()
    if (!q) return defaultOrderedRows

    return defaultOrderedRows.filter((r) => {
      const name = String(r.name ?? "").toLowerCase()
      const email = String(r.email ?? "").toLowerCase()
      const user = String(r.user ?? "").toLowerCase()
      return name.includes(q) || email.includes(q) || user.includes(q)
    })
  }, [defaultOrderedRows, search])

  const finalRows = React.useMemo<LicenseRow[]>(() => {
    // If no explicit sort requested, keep your default ordering
    if (!sortKey) return filteredRows

    const dir = sortDir === "asc" ? 1 : -1

    const compare = (a: LicenseRow, b: LicenseRow) => {
      const av = a[sortKey]
      const bv = b[sortKey]

      const as = String(av ?? "")
      const bs = String(bv ?? "")

      return as.localeCompare(bs, undefined, { sensitivity: "base" }) * dir
    }

    // When user sorts, let rows move freely (no Analyst pinning)
    return filteredRows.slice().sort(compare)
  }, [filteredRows, sortKey, sortDir])


  const onSort = (key: SortKey) => {
    if (sortKey !== key) {
      setSortKey(key)
      setSortDir("asc")
      return
    }
    // toggle direction
    setSortDir((d) => (d === "asc" ? "desc" : "asc"))
  }

  const SortableHead = ({ label, k }: { label: string; k: SortKey }) => {
    const active = sortKey === k
    return (
      <TableHead>
        <Button variant="ghost" className="px-0 h-auto font-medium" onClick={() => onSort(k)}>
          {label}
          <span className="ml-2 text-muted-foreground">{sortIcon(active, sortDir)}</span>
        </Button>
      </TableHead>
    )
  }

  return (
    <div className="w-full px-4">
      <Card className="shadow-md">
        <CardHeader>
          <CardTitle>License Reduction Data</CardTitle>

          {/* Description */}
          <div className="space-y-2 text-sm text-muted-foreground mt-2 mb-4">
            <p className="font-medium">Data includes:</p>
            <ul className="space-y-1">
              <li className="flex items-start">
                <span className="mr-1">•</span>
                Users who have logged into Spotfire in the last 90 days
              </li>
              <li className="flex items-start">
                <span className="mr-1">•</span>
                <span>
                  Users who perform 1+ analyst actions <strong>per day</strong> over the 90-day period are considered a
                  candidate for an Analyst license
                </span>
              </li>
            </ul>
          </div>

          {/* Controls */}
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <Select value={costCenter} onValueChange={setCostCenter}>
              <SelectTrigger className="w-[280px]">
                <SelectValue placeholder="Select a cost center" />
              </SelectTrigger>
              <SelectContent>
                {costCenters.map((cc) => (
                  <SelectItem key={cc} value={cc}>
                    {cc}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>

            <div className="flex items-center gap-2">
              <div className="relative sm:w-[340px]">
  <Input
    value={search}
    onChange={(e) => setSearch(e.target.value)}
    placeholder="Filter any column…"
    className="pr-8"
    disabled={!submittedPath || isBusy}
  />

  {search && (
    <button
      type="button"
      onClick={() => {
        setSearch("")
        // optional: refocus input after clearing
        queueMicrotask(() => {
          const el = document.activeElement as HTMLElement | null
          if (el?.tagName !== "INPUT") {
            const input = el?.closest("form")?.querySelector("input")
            input?.focus()
          }
        })
      }}
      className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
      aria-label="Clear search"
    >
      ✕
    </button>
  )}
</div>

              {sortKey && (
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    setSortKey(null)
                    setSortDir("asc")
                  }}
                >
                  Reset
                </Button>
              )}
            </div>
          </div>
          {!!search.trim() && (
            <div className="mt-2 text-sm text-muted-foreground">
              Showing {finalRows.length} of {defaultOrderedRows.length}
            </div>
          )}

          {/* Summary */}
          {costCenter && rows.length > 0 && (
            <div className="mt-4 p-3 bg-background rounded border">
              <div className="font-medium mb-2">License Distribution</div>

              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <span className="text-green-500">Analysts</span>
                  </div>
                  <div className="flex items-center gap-4 text-sm">
                    <span>{analystCount} users</span>
                    <span className="bg-green-100 dark:bg-green-900/20 px-2 py-1 rounded text-green-800 dark:text-green-200 font-medium">
                      {Math.round((analystCount / rows.length) * 100)}%
                    </span>
                  </div>
                </div>

                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <span className="text-blue-500">Consumers</span>
                  </div>
                  <div className="flex items-center gap-4 text-sm">
                    <span>{consumerCount} users</span>
                    <span className="bg-blue-100 dark:bg-blue-900/20 px-2 py-1 rounded text-blue-800 dark:text-blue-200 font-medium">
                      {Math.round((consumerCount / rows.length) * 100)}%
                    </span>
                  </div>
                </div>
              </div>
            </div>
          )}
        </CardHeader>

        <CardContent className="overflow-x-auto">
          {isLoading ? (
            <div className="text-center py-8">Loading…</div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <SortableHead label="Full Name" k="name" />
                  <SortableHead label="Username" k="user" />
                  <SortableHead label="Email" k="email" />
                  <SortableHead label="Cost Center" k="costCenterName" />
                  <SortableHead label="Department" k="departmentName" />
                  <SortableHead label="Title" k="title" />
                  <SortableHead label="Employee Status" k="statusName" />
                  <SortableHead label="Recommended License" k="recommendedAction" />
                </TableRow>
              </TableHeader>

              <TableBody>
                {finalRows.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={8} className="text-center py-4">
                      No matching results
                    </TableCell>
                  </TableRow>
                ) : (
                  finalRows.map((r, idx) => (
                    <TableRow key={idx}>
                      <TableCell>
                        {r.name === "Possibly Terminated" ? (
                          <span className="text-red-500 italic">{r.name}</span>
                        ) : (
                          <span>{r.name}</span>
                        )}
                      </TableCell>
                      <TableCell>{r.user}</TableCell>
                      <TableCell>{r.email}</TableCell>
                      <TableCell>{r.costCenterName}</TableCell>
                      <TableCell>{r.departmentName}</TableCell>
                      <TableCell>{r.title}</TableCell>
                      <TableCell>
                        {r.statusName === "Unknown" || r.statusName === "Terminated" ? (
                          <Badge variant="secondary" className="bg-red-100 text-red-800">
                            {r.statusName}
                          </Badge>
                        ) : (
                          <span>{r.statusName}</span>
                        )}
                      </TableCell>
                      <TableCell>
                        {r.recommendedAction === "Analyst" ? (
                          <Badge variant="secondary" className="bg-green-100 text-green-800">
                            {r.recommendedAction}
                          </Badge>
                        ) : (
                          <span>{r.recommendedAction}</span>
                        )}
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
