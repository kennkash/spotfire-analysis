function formatDateTime(value: any) {
  if (!value) return ""

  const d = new Date(value)
  if (isNaN(d.getTime())) return value // fallback if not valid

  const pad = (n: number) => String(n).padStart(2, "0")

  const month = pad(d.getMonth() + 1)
  const day = pad(d.getDate())
  const year = d.getFullYear()

  const hours = pad(d.getHours())
  const minutes = pad(d.getMinutes())
  const seconds = pad(d.getSeconds())

  return `${month}-${day}-${year} ${hours}:${minutes}:${seconds}`
}

{col.key === "logged_time"
  ? formatDateTime(r?.[col.key])
  : normalize(r?.[col.key])}




const columnConfig: { key: string; label: string }[] = [
  { key: "FULL_NAME", label: "Full Name" },
  { key: "user_name", label: "Username" },
  { key: "email", label: "Email" },
  { key: "cost_center_name", label: "Cost Center" },
  { key: "dept_name", label: "Department" },
  { key: "title", label: "Title" },
  { key: "STATUS_NAME", label: "Employee Status" },
  { key: "logged_time", label: "Last Viewed" },
]


{columnConfig.map((col) => (
  <TableHead key={col.key}>
    <Button
      variant="ghost"
      className="px-0 h-auto font-medium"
      onClick={() => onSort(col.key)}
    >
      {col.label}
      <span className="ml-2 text-muted-foreground">
        {sortIcon(sortKey === col.key, sortDir)}
      </span>
    </Button>
  </TableHead>
))}


{columnConfig.map((col) => (
  <TableCell key={col.key}>
    {normalize(r?.[col.key])}
  </TableCell>
))}



// spotfire-license-hub/src/components/report-views/report-views-view.tsx

"use client"

import * as React from "react"
import { useQuery } from "@tanstack/react-query"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"

import { getApiBase } from "@/lib/apiBase"

type JsonRow = Record<string, any>
type SortDir = "asc" | "desc"

async function fetchReportViews(reportPath: string): Promise<JsonRow[]> {
  const base = getApiBase()

  const res = await fetch(`${base}/v0/report-views`, {
    method: "POST",
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
    },
    body: JSON.stringify({ report_path: reportPath }),
  })

  if (!res.ok) {
    const text = await res.text().catch(() => "")
    throw new Error(text || `API error ${res.status}`)
  }

  return res.json()
}

function sortIcon(active: boolean, dir: SortDir) {
  if (!active) return ""
  return dir === "asc" ? "▲" : "▼"
}

function normalize(v: any) {
  if (v === null || v === undefined) return ""
  if (typeof v === "string") return v
  if (typeof v === "number" || typeof v === "boolean") return String(v)
  try {
    return JSON.stringify(v)
  } catch {
    return String(v)
  }
}

export default function ReportViewsView() {
  const [reportPath, setReportPath] = React.useState("")
  const [submittedPath, setSubmittedPath] = React.useState<string>("")
  const [search, setSearch] = React.useState("")
  const [sortKey, setSortKey] = React.useState<string | null>(null)
  const [sortDir, setSortDir] = React.useState<SortDir>("asc")

  const {
    data: rows = [],
    isLoading,
    isFetching,
    error,
  } = useQuery({
    queryKey: ["report-views", submittedPath],
    queryFn: () => fetchReportViews(submittedPath),
    enabled: !!submittedPath,
    refetchOnWindowFocus: false,
    staleTime: 0,
    retry: 1,
  })

  // Collect all keys across all rows (so we "include all keys in the JSON")
  const allKeys = React.useMemo(() => {
    const s = new Set<string>()
    for (const r of rows) Object.keys(r || {}).forEach((k) => s.add(k))
    return Array.from(s).sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }))
  }, [rows])

  // Reset search/sorting when a new report is submitted
  React.useEffect(() => {
    setSearch("")
    setSortKey(null)
    setSortDir("asc")
  }, [submittedPath])

  const filteredRows = React.useMemo(() => {
    const q = search.trim().toLowerCase()
    if (!q) return rows

    return rows.filter((r) =>
      allKeys.some((k) => normalize(r?.[k]).toLowerCase().includes(q))
    )
  }, [rows, search, allKeys])

  const finalRows = React.useMemo(() => {
    if (!sortKey) return filteredRows
    const dir = sortDir === "asc" ? 1 : -1

    const compare = (a: JsonRow, b: JsonRow) => {
      const av = normalize(a?.[sortKey])
      const bv = normalize(b?.[sortKey])
      return av.localeCompare(bv, undefined, { sensitivity: "base" }) * dir
    }

    return filteredRows.slice().sort(compare)
  }, [filteredRows, sortKey, sortDir])

  const onSubmit = () => {
    const v = reportPath.trim()
    if (!v) return
    setSubmittedPath(v)
  }

  const onSort = (key: string) => {
    if (sortKey !== key) {
      setSortKey(key)
      setSortDir("asc")
      return
    }
    setSortDir((d) => (d === "asc" ? "desc" : "asc"))
  }

  const SortableHead = ({ k }: { k: string }) => {
    const active = sortKey === k
    return (
      <TableHead className="whitespace-nowrap">
        <Button
          variant="ghost"
          className="px-0 h-auto font-medium"
          onClick={() => onSort(k)}
          title={`Sort by ${k}`}
        >
          {k}
          <span className="ml-2 text-muted-foreground">{sortIcon(active, sortDir)}</span>
        </Button>
      </TableHead>
    )
  }

  const LoadingIndicator = () => (
    <div className="flex items-center justify-center gap-3 py-10 text-muted-foreground">
      <span className="inline-block h-4 w-4 animate-spin rounded-full border-2 border-current border-t-transparent" />
      <span>Loading report views…</span>
    </div>
  )

  return (
    <div className="w-full px-4">
      <Card className="shadow-md">
        <CardHeader>
          <CardTitle>Report Views</CardTitle>

          {/* Controls */}
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between mt-3">
            <div className="flex items-center gap-2 w-full sm:w-auto">
              <Input
                value={reportPath}
                onChange={(e) => setReportPath(e.target.value)}
                placeholder="Enter report path (e.g. /Marketing/Reports/MyReport)"
                className="sm:w-[520px]"
                onKeyDown={(e) => {
                  if (e.key === "Enter") onSubmit()
                }}
                disabled={isLoading || isFetching}
              />
              <Button onClick={onSubmit} disabled={!reportPath.trim() || isLoading || isFetching}>
                Fetch
              </Button>
            </div>

            <div className="flex items-center gap-2">
              <Input
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Filter any column…"
                className="sm:w-[340px]"
                disabled={!submittedPath || isLoading || isFetching}
              />

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

          {!!submittedPath && (
            <div className="mt-2 text-sm text-muted-foreground flex items-center gap-2">
              <span>Report:</span>
              <Badge variant="secondary" className="font-mono">
                {submittedPath}
              </Badge>
              {!!search.trim() && (
                <span className="ml-2">
                  Showing {finalRows.length} of {rows.length}
                </span>
              )}
            </div>
          )}

          {error ? (
            <div className="mt-3 text-sm text-red-600">
              {(error as Error).message || "Failed to load report views"}
            </div>
          ) : null}
        </CardHeader>

        <CardContent className="overflow-x-auto">
          {isLoading || isFetching ? (
            <LoadingIndicator />
          ) : !submittedPath ? (
            <div className="text-center py-10 text-muted-foreground">
              Enter a report path above and press <span className="font-medium">Enter</span>.
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  {allKeys.map((k) => (
                    <SortableHead key={k} k={k} />
                  ))}
                </TableRow>
              </TableHeader>

              <TableBody>
                {finalRows.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={Math.max(allKeys.length, 1)} className="text-center py-6">
                      No matching results
                    </TableCell>
                  </TableRow>
                ) : (
                  finalRows.map((r, idx) => (
                    <TableRow key={idx}>
                      {allKeys.map((k) => (
                        <TableCell key={k} className="align-top">
                          <span className="whitespace-pre-wrap break-words">{normalize(r?.[k])}</span>
                        </TableCell>
                      ))}
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
