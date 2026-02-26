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
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue
} from "@/components/ui/select"

import { getApiBase } from "@/lib/apiBase"

type JsonRow = Record<string, any> & {
    view_count?: number
}
type SortDir = "asc" | "desc"

async function fetchReportViews(reportPath: string, days: string = "30"): Promise<JsonRow[]> {
    const base = getApiBase()

    const res = await fetch(`${base}/v0/report-views`, {
        method: "POST",
        credentials: "include",
        headers: {
            "Content-Type": "application/json",
            "Cache-Control": "no-store",
        },
        body: JSON.stringify({
            report_path: reportPath,
            days: parseInt(days)
        }),
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

    // Handle different input types
    let dateTime;
    if (typeof value === 'string') {
        // Parse ISO 8601 string directly
        dateTime = new Date(value);
    } else {
        // Handle other cases (e.g., Unix timestamp, JS Date object)
        dateTime = new Date(value);
    }

    if (isNaN(dateTime.getTime())) return value; // fallback if not valid

    const pad = (n: number) => String(n).padStart(2, "0");

    const month = pad(dateTime.getUTCMonth() + 1);
    const day = pad(dateTime.getUTCDate());
    const year = dateTime.getUTCFullYear();

    const hours = dateTime.getUTCHours();
    const minutes = pad(dateTime.getUTCMinutes());
    const seconds = pad(dateTime.getUTCSeconds());

    // Convert to 12-hour format
    const ampm = hours >= 12 ? 'PM' : 'AM';
    const displayHours = hours % 12 || 12; // Convert 0 to 12 for 12PM

    return `${month}/${day}/${year} ${pad(displayHours)}:${minutes}:${seconds} ${ampm}`;
}

export default function ReportViewsView() {
    // Draft inputs (user can change these freely)
    const [reportPath, setReportPath] = React.useState("")
    const [timeRange, setTimeRange] = React.useState("30")

    // Submitted inputs (only change when user clicks Fetch / presses Enter)
    const [submittedPath, setSubmittedPath] = React.useState<string>("")
    const [submittedTimeRange, setSubmittedTimeRange] = React.useState<string>("30")

    const [search, setSearch] = React.useState("")
    const [sortKey, setSortKey] = React.useState<string | null>(null)
    const [sortDir, setSortDir] = React.useState<SortDir>("asc")

    const { data: rows = [], isLoading, isFetching, error } = useQuery({
        queryKey: ["report-views", submittedPath, submittedTimeRange],
        queryFn: () => fetchReportViews(submittedPath, submittedTimeRange),
        enabled: !!submittedPath, // only fetch after a path has been submitted
        refetchOnWindowFocus: false,
        staleTime: 0,
        retry: 1,
    })

    const columnConfig: { key: string; label: string }[] = [
        { key: "FULL_NAME", label: "Full Name" },
        { key: "user_name", label: "Username" },
        { key: "email", label: "Email" },
        { key: "cost_center_name", label: "Cost Center" },
        { key: "dept_name", label: "Department" },
        { key: "title", label: "Title" },
        { key: "view_count", label: "Views" },
        { key: "logged_time", label: "Last Viewed" },
    ]

    // Reset search/sorting when a new report OR timeframe is submitted (i.e., when data actually changes)
    React.useEffect(() => {
        setSearch("")
        setSortKey(null)
        setSortDir("asc")
    }, [submittedPath, submittedTimeRange])

    const filterKeys = React.useMemo(
        () => columnConfig.map(c => c.key),
        [columnConfig]
    )

    const filteredRows = React.useMemo(() => {
        const q = normalize(search).toLowerCase()
        if (!q) return rows

        return rows.filter((r) =>
            filterKeys.some((k) => normalize(r?.[k]).toLowerCase().includes(q))
        )
    }, [rows, search, filterKeys])

    const finalRows = React.useMemo(() => {
        if (!sortKey) return filteredRows
        const dir = sortDir === "asc" ? 1 : -1

        const compare = (a: JsonRow, b: JsonRow) => {
            if (sortKey === "view_count") {
                // Handle view_count as a number
                const countA = Number(a?.[sortKey] || 0)
                const countB = Number(b?.[sortKey] || 0)
                return (countA - countB) * dir
            }

            // Handle other columns as strings
            const av = normalize(a?.[sortKey])
            const bv = normalize(b?.[sortKey])
            return av.localeCompare(bv, undefined, { sensitivity: "base" }) * dir
        }

        return [...filteredRows].sort(compare)
    }, [filteredRows, sortKey, sortDir])

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
    const totalViews = React.useMemo(() => {
        return rows.reduce((sum, row) => {
            // Ensure view_count exists and is a valid number
            const count = Number(row?.view_count || 0)
            return sum + count
        }, 0)
    }, [rows])

    const reportNotFound = submittedPath && !isLoading && !isFetching && rows.length === 0

    // Disabled until reportPath OR timeRange differs from last submitted values
    const hasSubmitted = !!submittedPath
    const isBusy = isLoading || isFetching

    const trimmedPath = reportPath.trim()
    const canFetch =
        !!trimmedPath &&
        !isBusy &&
        (!hasSubmitted || trimmedPath !== submittedPath || timeRange !== submittedTimeRange)

    const onFetch = () => {
        const v = reportPath.trim()
        if (!v) return

        setSubmittedPath(v)
        setSubmittedTimeRange(timeRange)
        setReportPath(v) // keeps input normalized
    }

    const searchRef = React.useRef<HTMLInputElement | null>(null)


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
                                        <br />
                                        <br />
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
                            <li>Report views from the past <strong>{timeRange} days</strong></li>
                            <li>
                                One entry per user, displaying their <u>total view count</u> & <u>most recent view</u> of the report
                            </li>
                        </ul>
                    </div>

                    {/* Controls */}
                    <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between mt-3">
                        <div className="flex items-center gap-2 w-full sm:w-auto">
                            <Input
                                value={reportPath}
                                onChange={(e) => setReportPath(e.target.value)}
                                placeholder="Enter report path (e.g. /31_S.LSI/04 Team/Spotfire/Jane Doe/Spotfire_Analysis)"
                                className="sm:w-[540px]"
                                onKeyDown={(e) => {
                                    if (e.key === "Enter") onFetch()
                                }}
                                disabled={isBusy}
                            />
                            <Select value={timeRange} onValueChange={setTimeRange}>
                                <SelectTrigger className="w-[140px]">
                                    <SelectValue placeholder="Time range" />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="30">Last 30 Days</SelectItem>
                                    <SelectItem value="60">Last 60 Days</SelectItem>
                                    <SelectItem value="90">Last 90 Days</SelectItem>
                                </SelectContent>
                            </Select>
                            <Button onClick={onFetch} disabled={!canFetch} className="hover:cursor-pointer">
                                Fetch
                            </Button>
                        </div>

                        <div className="flex items-center gap-2">
                            <div className="relative sm:w-[340px]">
                                <Input
                                    ref={searchRef}
                                    value={search}
                                    onChange={(e) => setSearch(e.target.value)}
                                    placeholder="Filter any column…"
                                    className="pr-8"
                                    disabled={!submittedPath || isBusy}
                                />

                                {!!search.trim() && (
                                    <button
                                        type="button"
                                        onClick={() => {
                                            setSearch("")
                                            // Focus AFTER React applies the state update
                                            requestAnimationFrame(() => searchRef.current?.focus())
                                        }}
                                        className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                                        aria-label="Clear search"
                                        title="Clear"
                                    >
                                        ✕
                                    </button>
                                )}
                            </div>

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
                                    disabled={!submittedPath || isBusy}
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

                            <div className="space-y-2">
                                <div className="flex items-center justify-between">
                                    <div className="flex items-center gap-2">
                                        <span className="text-green-500">Total Views</span>
                                    </div>
                                    <div className="flex items-center gap-4 text-sm">
                                        <span className="bg-green-100 dark:bg-green-900/20 px-2 py-1 rounded text-green-800 dark:text-green-200 font-medium">
                                            {totalViews}
                                        </span>
                                    </div>
                                </div>

                                <div className="flex items-center justify-between">
                                    <div className="flex items-center gap-2">
                                        <span className="text-blue-500">Unique Viewers</span>
                                    </div>
                                    <div className="flex items-center gap-4 text-sm">
                                        <span className="bg-blue-100 dark:bg-blue-900/20 px-2 py-1 rounded text-blue-800 dark:text-blue-200 font-medium">
                                            {totalUniqueViewers}
                                        </span>
                                    </div>
                                </div>
                            </div>
                        </div>
                    )}
                </CardHeader>

                <CardContent className="overflow-x-auto">
                    {isBusy ? (
                        <LoadingIndicator />
                    ) : !submittedPath ? (
                        <div className="text-center py-10 text-muted-foreground">
                            Enter a report path above and click <span className="font-medium">Fetch</span> or press <span className="font-medium">Enter</span>.
                        </div>
                    ) : reportNotFound ? (
                        <div className="py-16 text-center">
                            <div className="text-lg font-semibold text-red-600">No Report Views Found</div>
                            <div className="text-sm text-muted-foreground mt-2">
                                No views were found for this report path. Please double check the path and try again.
                            </div>
                            <Button
                                variant="outline"
                                onClick={() => {
                                    setReportPath("")
                                    setSubmittedPath("")
                                    setSubmittedTimeRange("30")
                                    setTimeRange("30")
                                }}
                                className="mt-4"
                            >
                                Try Another Path
                            </Button>
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
