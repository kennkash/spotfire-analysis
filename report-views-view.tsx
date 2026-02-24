 how can I make the summary and report badge clear out when the input for the report path is cleared?

   // spotfire-license-hub/src/components/report-views/report-views-view.tsx

"use client"

import * as React from "react"
import { useQuery } from "@tanstack/react-query"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog"
import { Info } from "lucide-react"


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

    const LoadingIndicator = () => (
        <div className="flex items-center justify-center gap-3 py-10 text-muted-foreground">
            <span className="inline-block h-4 w-4 animate-spin rounded-full border-2 border-current border-t-transparent" />
            <span>Loading report views…</span>
        </div>
    )

    const totalUniqueViewers = rows.length

    const reportNotFound = submittedPath && !isLoading && !isFetching && rows.length === 0

    return (
        <div className="w-full px-4">
            <Card className="shadow-md">
                <CardHeader>
                    <div className="flex items-center justify-between">
                        <CardTitle>Report Views</CardTitle>

                        <Dialog>
                            <DialogTrigger asChild>
                                <Button
                                    variant="ghost"
                                    size="icon"
                                    className="h-8 w-8"
                                    aria-label="How to find a report path"
                                    title="How to find a report path"
                                >
                                    <Info className="h-4 w-4" />
                                </Button>
                            </DialogTrigger>

                            <DialogContent className="sm:max-w-[620px]">
                                <DialogHeader>
                                    <DialogTitle>Locate a Report&apos;s Path</DialogTitle>
                                </DialogHeader>

                                <div className="space-y-3 text-sm text-muted-foreground">
                                    <ol className="space-y-2 list-decimal pl-5">
                                        <li>Navigate to Spotfire and open the report</li>
                                        <li>
                                            In the toolbar, select <strong>File &gt; Document Properties</strong>
                                        </li>
                                        <li>In the Document Properties menu, select the <strong>Library</strong> tab</li>
                                        <li>
                                            Copy the report path that follows <code>:analysis:</code> in the Library URL
                                        </li>
                                    </ol>

                                    <div className="rounded border bg-background p-3">
                                        <div className="text-xs uppercase tracking-wide text-muted-foreground mb-4"><Badge variant="secondary">
                                            Example
                                        </Badge></div>
                                        <span className="font-bold">Library URL: </span>
                                        <code className="text-xs break-all">
                                            tibcospotfire:server:http\://105.195.16.62\:8081/:analysis:/31_S.LSI/04 Team/Spotfire/Jane Doe/Spotfire_Analysis
                                        </code>
                                        <br></br>
                                        <br></br>
                                        <span className="font-bold">Report Path: </span>
                                        <code className="text-xs break-all">
                                            /31_S.LSI/04 Team/Spotfire/Jane Doe/Spotfire_Analysis
                                        </code>
                                    </div>
                                </div>
                            </DialogContent>
                        </Dialog>
                    </div>
                    {/* Description */}
                    <div className="space-y-2 text-sm text-muted-foreground mt-2 mb-4">
                        <p className="font-medium">Data includes:</p>
                        <ul className="space-y-2 list-disc pl-5">
                            <li>Displays report views from the past <strong>30 days</strong></li>
                            <li>
                                Shows one entry per user (<i>unique</i> viewers only)
                            </li>
                            <li>Extracts each user's most recent view of the report</li>
                        </ul>
                    </div>

                    {/* Controls */}
                    <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between mt-3">
                        <div className="flex items-center gap-2 w-full sm:w-auto">
                            <Input
                                value={reportPath}
                                onChange={(e) => setReportPath(e.target.value)}
                                placeholder="Enter report path (e.g. /31_S.LSI/04 Team/Spotfire/Jane Doe/Spotfire_Analysis)"
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

                        <div className="flex flex-col gap-1">
                            <Input
                                value={search}
                                onChange={(e) => setSearch(e.target.value)}
                                placeholder="Filter any column…"
                                className="sm:w-[340px]"
                                disabled={!submittedPath || isLoading || isFetching}
                            />
                            {!!search.trim() && (
                                <span className="mt-2 text-sm text-muted-foreground flex items-center gap-2">
                                    Showing {finalRows.length} of {rows.length}
                                </span>
                            )}

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
                        </div>
                    )}

                    {error ? (
                        <div className="mt-3 text-sm text-red-600">
                            {(error as Error).message || "Failed to load report views"}
                        </div>
                    ) : null}

                    {!!submittedPath && rows.length > 0 && (
                        <div className="mt-4 p-3 bg-background rounded border">
                            <div className="font-medium mb-2">Summary</div>

                            <div className="flex items-center justify-between text-sm">
                                <span>Total unique viewers</span>
                                <span className="bg-green-100 dark:bg-green-900/20 px-2 py-1 rounded text-green-800 dark:text-green-200 font-medium">{totalUniqueViewers}</span>
                            </div>
                        </div>
                    )}
                </CardHeader>

                <CardContent className="overflow-x-auto">
                    {isLoading || isFetching ? (
                        <LoadingIndicator />
                    ) : !submittedPath ? (
                        <div className="text-center py-10 text-muted-foreground">
                            Enter a report path above and press <span className="font-medium">Enter</span>.
                        </div>
                    ) : reportNotFound ? (
                        <div className="py-16 text-center">
                            <div className="text-lg font-semibold text-red-600">No Report Views Found</div>
                            <div className="text-sm text-muted-foreground mt-2">
                                No views were found for this report path. Please double check the path and try again.
                            </div>
                        </div>
                    ) : (
                        <Table>
                            <TableHeader>
                                <TableRow>
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
                                </TableRow>
                            </TableHeader>

                            <TableBody>
                                {finalRows.length === 0 ? (
                                    <TableRow>
                                        <TableCell colSpan={columnConfig.length} className="text-center py-6">
                                            No matching results
                                        </TableCell>
                                    </TableRow>
                                ) : (
                                    finalRows.map((r, idx) => (
                                        <TableRow key={idx}>
                                            {columnConfig.map((col) => (
                                                <TableCell key={col.key}>
                                                    {col.key === "logged_time"
                                                        ? formatDateTime(r?.[col.key])
                                                        : normalize(r?.[col.key])}
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
