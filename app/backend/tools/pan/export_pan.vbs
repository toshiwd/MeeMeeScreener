Option Explicit

'============================================================
' export_pan.vbs (Pan ActiveMarket -> TXT)
'
' Output line:
'   code,YYYY/MM/DD,Open,High,Low,Close,Volume
'
' Behavior:
' - Fast incremental append using manifest (no full scan)
' - Detect split/adjustment change (suspect) and skip append
'============================================================

Const PROGRESS_INTERVAL = 500
Const ADJUST_DIFF_THRESHOLD = 0.005
Const ABS_DIFF_THRESHOLD = 0.01
Const COM_CREATE_RETRY_COUNT = 15
Const COM_CREATE_RETRY_SLEEP_MS = 1000
Const USE_ADJUSTED_PRICE = True
Const MANIFEST_NAME = "_manifest.csv"
Const SPLIT_SUSPECTS_NAME = "_split_suspects.csv"
Const PROGRESS_FILE_NAME = "vbs_progress.json"
Const TAIL_READ_BYTES = 4096

Const ForReading   = 1
Const ForWriting   = 2
Const ForAppending = 8

Dim fso
Set fso = CreateObject("Scripting.FileSystemObject")

Call Main()

Sub Main()
    Ensure32BitCscriptHostOrRelaunch

    Dim baseFolder, codesFile, outFolder
    Dim cal, prices
    Dim tsCodes
    Dim totalCount, okCount, errCount, splitCount
    Dim sLine, code
    Dim manifestPath, splitPath
    Dim progressPath
    Dim manifest

    totalCount = 0
    okCount = 0
    errCount = 0
    splitCount = 0

    baseFolder = fso.GetParentFolderName(WScript.ScriptFullName)
    codesFile = baseFolder & "\code.txt"
    outFolder = fso.GetAbsolutePathName(baseFolder & "\..\data\txt")
    If WScript.Arguments.Count >= 1 Then
        codesFile = WScript.Arguments(0)
    End If
    If WScript.Arguments.Count >= 2 Then
        outFolder = WScript.Arguments(1)
    End If
    manifestPath = outFolder & "\" & MANIFEST_NAME
    splitPath = outFolder & "\" & SPLIT_SUSPECTS_NAME
    progressPath = outFolder & "\" & PROGRESS_FILE_NAME

    ' Write a boot record early so the UI can detect "script started" even when COM init fails.
    WriteProgressFile progressPath, "booting", "", 0, 0, 0, 0, ""

    If Not fso.FileExists(codesFile) Then
        SafeOut "ERROR: code.txt not found: " & codesFile
        WriteProgressFile progressPath, "error", "", 0, 0, 0, 0, "code.txt not found"
        WScript.Quit 1
    End If

    If Not fso.FolderExists(outFolder) Then
        Dim parentFolder
        parentFolder = fso.GetParentFolderName(outFolder)
        On Error Resume Next
        If Len(parentFolder) > 0 And Not fso.FolderExists(parentFolder) Then
            fso.CreateFolder parentFolder
        End If
        If Not fso.FolderExists(outFolder) Then
            fso.CreateFolder outFolder
        End If
        On Error GoTo 0
    End If

    Set cal = Nothing
    Set prices = Nothing
    If Not CreateActiveMarketObjectsWithRetry(cal, prices) Then
        SafeOut "ERROR: ActiveMarket objects could not be created."
        SafeOut "HOST : " & WScript.FullName
        SafeOut "HINT : Use 32bit cscript (SysWOW64)."
        SafeOut "TRY  : %SystemRoot%\SysWOW64\cscript.exe //nologo ""export_pan.vbs"""
        WriteProgressFile progressPath, "error", "", 0, 0, 0, 0, "ActiveMarket COM init failed"
        WScript.Quit 1
    End If

    Set manifest = LoadManifest(manifestPath)
    Set tsCodes = fso.OpenTextFile(codesFile, ForReading, False)

    SafeOut "=== Pan Export Start ==="
    SafeOut "BASE : " & baseFolder
    SafeOut "CODES: " & codesFile
    SafeOut "OUT  : " & outFolder
    SafeOut "HOST : " & WScript.FullName
    WriteProgressFile progressPath, "starting", "", totalCount, okCount, errCount, splitCount, ""

    Do While Not tsCodes.AtEndOfStream
        sLine = Trim(RemoveBOM(tsCodes.ReadLine))
        If Len(sLine) > 0 Then
            If Left(sLine, 1) <> "#" And Left(sLine, 1) <> "'" Then
                code = NormalizeCode(sLine)
                If Len(code) > 0 Then
                    totalCount = totalCount + 1
                    WriteProgressFile progressPath, "exporting", code, totalCount, okCount, errCount, splitCount, ""
                    Dim splitSuspected
                    splitSuspected = False
                    If ExportOneCode_Incremental(code, outFolder, cal, prices, manifest, splitPath, splitSuspected) Then
                        okCount = okCount + 1
                    Else
                        If splitSuspected Then
                            splitCount = splitCount + 1
                        Else
                            errCount = errCount + 1
                        End If
                    End If
                    WriteProgressFile progressPath, "exporting", code, totalCount, okCount, errCount, splitCount, ""
                End If
            End If
        End If
    Loop

    tsCodes.Close
    SaveManifest manifestPath, manifest

    SafeOut "=== Pan Export Done ==="
    SafeOut "TOTAL: " & totalCount
    SafeOut "OK   : " & okCount
    SafeOut "ERR  : " & errCount
    SafeOut "SPLIT_SUSPECT: " & splitCount
    SafeOut "SUMMARY: total=" & totalCount & " ok=" & okCount & " err=" & errCount & " split=" & splitCount
    WriteProgressFile progressPath, "done", "", totalCount, okCount, errCount, splitCount, ""

    If errCount > 0 Then
        SafeOut "WARN: errors detected"
    End If

    If errCount > 0 And okCount = 0 Then
        WScript.Quit 2
    Else
        WScript.Quit 0
    End If
End Sub

'============================================================
' Progress file (used by UI polling if stdout streaming is not available)
'============================================================
Sub WriteProgressFile(ByVal path, ByVal phase, ByVal currentCode, ByVal startedCount, ByVal okCount, ByVal errCount, ByVal splitCount, ByVal errorMessage)
    On Error Resume Next
    Dim ts, processedCount, updatedAt, jsonText
    processedCount = CLng(okCount) + CLng(errCount) + CLng(splitCount)
    updatedAt = FormatTimestamp(Now)
    Set ts = fso.OpenTextFile(path, ForWriting, True)
    If Err.Number <> 0 Then
        Err.Clear
        Exit Sub
    End If
    jsonText = "{""phase"":""" & CleanJson(phase) & """,""current"":""" & CleanJson(currentCode) & """,""started"": " & startedCount & ",""processed"": " & processedCount & ",""ok"": " & okCount & ",""err"": " & errCount & ",""split"": " & splitCount & ",""error"":""" & CleanJson(errorMessage) & """,""updatedAt"":""" & CleanJson(updatedAt) & """}"
    ts.WriteLine jsonText
    ts.Close
    On Error GoTo 0
End Sub

Function CleanJson(ByVal value)
    Dim s
    s = CStr(value)
    s = Replace(s, "\", "\\")
    s = Replace(s, Chr(34), "\""")
    s = Replace(s, vbCrLf, " ")
    s = Replace(s, vbLf, " ")
    CleanJson = s
End Function

'============================================================
' 32-bit cscript check (relaunch if needed)
'============================================================
Sub Ensure32BitCscriptHostOrRelaunch()
    Dim shell, env, sysRoot, hostPath
    Dim is64OS, isCscript, isSystem32Host
    Dim targetCscript, cmd, i

    hostPath = LCase(WScript.FullName)
    isCscript = (InStr(hostPath, "\cscript.exe") > 0)

    Set shell = CreateObject("WScript.Shell")
    Set env = shell.Environment("PROCESS")

    sysRoot = env("SystemRoot")
    If Len(sysRoot) = 0 Then sysRoot = "C:\Windows"

    is64OS = False
    If UCase(env("PROCESSOR_ARCHITECTURE")) = "AMD64" Then is64OS = True
    If Len(env("PROCESSOR_ARCHITEW6432")) > 0 Then is64OS = True

    isSystem32Host = (InStr(hostPath, "\system32\") > 0)

    If is64OS And isSystem32Host Then
        targetCscript = sysRoot & "\SysWOW64\cscript.exe"
        If fso.FileExists(targetCscript) Then
            cmd = Q(targetCscript) & " //nologo " & Q(WScript.ScriptFullName)
            If WScript.Arguments.Count > 0 Then
                For i = 0 To WScript.Arguments.Count - 1
                    cmd = cmd & " " & Q(WScript.Arguments(i))
                Next
            End If
            shell.Run cmd, 1, True
            WScript.Quit 0
        End If
    End If

    If Not isCscript Then
        targetCscript = sysRoot & "\SysWOW64\cscript.exe"
        If Not fso.FileExists(targetCscript) Then
            targetCscript = sysRoot & "\System32\cscript.exe"
        End If

        cmd = Q(targetCscript) & " //nologo " & Q(WScript.ScriptFullName)
        If WScript.Arguments.Count > 0 Then
            For i = 0 To WScript.Arguments.Count - 1
                cmd = cmd & " " & Q(WScript.Arguments(i))
            Next
        End If

        shell.Run cmd, 1, True
        WScript.Quit 0
    End If
End Sub

'============================================================
' ActiveMarket objects
'============================================================
Function CreateActiveMarketObjectsWithRetry(ByRef cal, ByRef prices)
    Dim i
    CreateActiveMarketObjectsWithRetry = False
    For i = 1 To COM_CREATE_RETRY_COUNT
        On Error Resume Next
        Err.Clear
        Set cal = Nothing
        Set prices = Nothing
        Set cal = CreateObjectByCandidates(Array("ActiveMarket.Calendar", "ActiveMarket.Calendar.1"))
        Set prices = CreateObjectByCandidates(Array("ActiveMarket.Prices", "ActiveMarket.Prices.1"))
        If (Err.Number = 0) And (Not cal Is Nothing) And (Not prices Is Nothing) Then
            CreateActiveMarketObjectsWithRetry = True
            On Error GoTo 0
            Exit Function
        End If
        Err.Clear
        On Error GoTo 0
        WScript.Sleep COM_CREATE_RETRY_SLEEP_MS
    Next
End Function

Function CreateObjectByCandidates(ByVal progIds)
    Dim i, obj
    Set obj = Nothing
    On Error Resume Next
    For i = LBound(progIds) To UBound(progIds)
        Err.Clear
        Set obj = CreateObject(CStr(progIds(i)))
        If (Err.Number = 0) And (Not obj Is Nothing) Then
            Exit For
        End If
        Set obj = Nothing
    Next
    On Error GoTo 0
    Set CreateObjectByCandidates = obj
End Function

'============================================================
' Adjusted price
'============================================================
Sub ConfigureAdjustedPriceIfAvailable(ByRef prices, ByVal code)
    If Not USE_ADJUSTED_PRICE Then Exit Sub
    On Error Resume Next
    Err.Clear
    prices.AdjustExRights = True
    If Err.Number <> 0 Then
        Err.Clear
        prices.AdjustExRights = 1
    End If
    If Err.Number <> 0 Then
        SafeOut "WARN : " & code & " : AdjustExRights not supported -> continue RAW"
        Err.Clear
    End If
    On Error GoTo 0
End Sub

'============================================================
' Export 1 code incrementally
'============================================================
Function ExportOneCode_Incremental(ByVal code, ByVal outFolder, ByRef cal, ByRef prices, ByRef manifest, ByVal splitPath, ByRef splitSuspected)
    Dim beginPos, endPos, pos, startPos
    Dim rawName, safeName, outPath
    Dim canAppend, lastDate, lastDateStr, lastClose
    Dim firstDate, firstClose
    Dim d, o, h, l, c, v
    Dim sDate, outLine
    Dim writtenCount, lastWrittenDate, lastWrittenClose
    Dim panMatchPos, panMatchDate, panMatchClose
    Dim splitReason, diffRel

    ExportOneCode_Incremental = False
    splitSuspected = False
    writtenCount = 0
    lastWrittenDate = ""
    lastWrittenClose = ""
    lastDate = Empty
    lastDateStr = ""
    lastClose = 0
    firstDate = ""
    firstClose = ""
    canAppend = False
    startPos = 0

    SafeOut "START: " & code

    Call ConfigureAdjustedPriceIfAvailable(prices, code)

    On Error Resume Next
    Err.Clear
    prices.Read code
    If Err.Number <> 0 Then
        SafeOut "ERROR: " & code & " : prices.Read failed : " & Err.Description
        Err.Clear
        On Error GoTo 0
        Exit Function
    End If
    On Error GoTo 0

    On Error Resume Next
    Err.Clear
    beginPos = prices.Begin
    If Err.Number <> 0 Then
        SafeOut "ERROR: " & code & " : prices.Begin failed : " & Err.Description
        Err.Clear
        On Error GoTo 0
        Exit Function
    End If

    endPos = prices.End
    If Err.Number <> 0 Then
        SafeOut "ERROR: " & code & " : prices.End failed : " & Err.Description
        Err.Clear
        On Error GoTo 0
        Exit Function
    End If
    On Error GoTo 0

    If beginPos > endPos Then
        SafeOut "WARN : " & code & " : no data"
        Exit Function
    End If

    rawName = ""
    On Error Resume Next
    Err.Clear
    rawName = prices.Name
    Err.Clear
    On Error GoTo 0

    safeName = SanitizeFileName(rawName)
    If Len(safeName) = 0 Then safeName = "NONAME"

    outPath = outFolder & "\" & code & "_" & safeName & ".txt"
    startPos = beginPos

    If fso.FileExists(outPath) Then
        If manifest.Exists(code) Then
            Dim rec
            Set rec = manifest(code)
            If rec.Exists("lastDate") Then lastDateStr = CStr(rec("lastDate"))
            If rec.Exists("lastClose") Then lastClose = ToDouble(rec("lastClose"))
            If rec.Exists("firstDate") Then firstDate = CStr(rec("firstDate"))
            If rec.Exists("firstClose") Then firstClose = CStr(rec("firstClose"))
        End If

        If Len(lastDateStr) = 0 Or lastClose = 0 Then
            If Not ReadLastRowInfoFast(outPath, lastDateStr, lastClose) Then
                SafeOut "ERROR: " & code & " : last date not found"
                Exit Function
            End If
        End If

        lastDate = ParseYYYYMMDDToDate(lastDateStr)
        If Not IsDate(lastDate) Then
            SafeOut "ERROR: " & code & " : invalid last date"
            Exit Function
        End If

        panMatchPos = FindPosByDate(cal, beginPos, endPos, lastDate)
        If panMatchPos < 0 Then
            splitReason = "date_missing"
            splitSuspected = True
            AppendSplitSuspect splitPath, code, lastDateStr, lastClose, "", "", 0, splitReason
            SafeOut "SPLIT : " & code & " : " & splitReason
            Exit Function
        End If

        panMatchDate = ""
        panMatchClose = 0
        On Error Resume Next
        Err.Clear
        d = cal.Date(panMatchPos)
        If Err.Number = 0 Then
            panMatchDate = FormatYYYYMMDD(d)
            panMatchClose = ToDouble(prices.Close(panMatchPos))
        End If
        Err.Clear
        On Error GoTo 0

        splitReason = ""
        If IsSplitSuspect(lastDateStr, lastClose, panMatchDate, panMatchClose, splitReason) Then
            diffRel = 0
            If panMatchClose <> 0 Then
                diffRel = Abs(ToDouble(lastClose) - ToDouble(panMatchClose)) / panMatchClose
            End If
            splitSuspected = True
            AppendSplitSuspect splitPath, code, lastDateStr, lastClose, panMatchDate, panMatchClose, diffRel, splitReason
            SafeOut "SPLIT : " & code & " : " & splitReason
            Exit Function
        End If

        canAppend = True
        startPos = panMatchPos + 1
    End If

    Dim tsOut
    On Error Resume Next
    Err.Clear
    If canAppend Then
        Set tsOut = fso.OpenTextFile(outPath, ForAppending, True)
    Else
        Set tsOut = fso.OpenTextFile(outPath, ForWriting, True)
    End If
    If Err.Number <> 0 Then
        SafeOut "ERROR: " & code & " : open write failed : " & Err.Description
        Err.Clear
        On Error GoTo 0
        Exit Function
    End If
    On Error GoTo 0

    If startPos < beginPos Then startPos = beginPos

    If startPos <= endPos Then
        For pos = startPos To endPos
            On Error Resume Next
            Err.Clear
            If Not prices.IsClosed(pos) Then
                d = cal.Date(pos)
                If Err.Number = 0 Then
                    o = prices.Open(pos)
                    h = prices.High(pos)
                    l = prices.Low(pos)
                    c = prices.Close(pos)
                    v = prices.Volume(pos)

                    sDate = FormatYYYYMMDD(d)
                    outLine = code & "," & sDate & "," & CStr(o) & "," & CStr(h) & "," & CStr(l) & "," & CStr(c) & "," & CStr(v)
                    tsOut.WriteLine outLine

                    If writtenCount = 0 And (Not canAppend) Then
                        firstDate = sDate
                        firstClose = CStr(c)
                    End If

                    writtenCount = writtenCount + 1
                    lastWrittenDate = sDate
                    lastWrittenClose = CStr(c)
                    If (writtenCount Mod PROGRESS_INTERVAL) = 0 Then
                        SafeOut "  " & code & " ... " & writtenCount & " lines"
                    End If
                Else
                    Err.Clear
                End If
            End If
            On Error GoTo 0
        Next
    End If

    tsOut.Close

    Dim updatedAt
    updatedAt = FormatTimestamp(Now)
    Dim rec2
    Set rec2 = GetOrCreateManifestRecord(manifest, code)
    rec2("filePath") = outPath
    If Len(firstDate) > 0 Then rec2("firstDate") = firstDate
    If Len(firstClose) > 0 Then rec2("firstClose") = CStr(firstClose)
    If Len(lastWrittenDate) > 0 Then
        rec2("lastDate") = lastWrittenDate
        rec2("lastClose") = CStr(lastWrittenClose)
    Else
        rec2("lastDate") = lastDateStr
        rec2("lastClose") = CStr(lastClose)
    End If
    rec2("updatedAt") = updatedAt

    If writtenCount > 0 Then
        SafeOut "OK   : " & code & " : +" & writtenCount
    Else
        SafeOut "OK   : " & code & " : +0"
    End If

    ExportOneCode_Incremental = True
End Function

'============================================================
' Manifest
'============================================================
Function LoadManifest(ByVal path)
    Dim dict
    Set dict = CreateObject("Scripting.Dictionary")
    If Not fso.FileExists(path) Then
        Set LoadManifest = dict
        Exit Function
    End If

    Dim ts, line, parts
    On Error Resume Next
    Set ts = fso.OpenTextFile(path, ForReading, False)
    If Err.Number <> 0 Then
        Err.Clear
        Set LoadManifest = dict
        Exit Function
    End If
    On Error GoTo 0

    If Not ts.AtEndOfStream Then
        ts.ReadLine
    End If

    Do While Not ts.AtEndOfStream
        line = Trim(ts.ReadLine)
        If Len(line) > 0 Then
            parts = Split(line, ",")
            If UBound(parts) >= 5 Then
                Dim code, rec
                Dim lastClose, firstDateIdx, firstCloseIdx, filePathIdx, updatedAtIdx
                code = Trim(parts(0))
                If Len(code) > 0 Then
                    Set rec = CreateObject("Scripting.Dictionary")
                    rec.Add "lastDate", Trim(parts(1))
                    If UBound(parts) >= 6 Then
                        lastClose = Trim(parts(2))
                        firstDateIdx = 3
                        firstCloseIdx = 4
                        filePathIdx = 5
                        updatedAtIdx = 6
                    Else
                        lastClose = ""
                        firstDateIdx = 2
                        firstCloseIdx = 3
                        filePathIdx = 4
                        updatedAtIdx = 5
                    End If
                    rec.Add "lastClose", lastClose
                    rec.Add "firstDate", Trim(parts(firstDateIdx))
                    rec.Add "firstClose", Trim(parts(firstCloseIdx))
                    rec.Add "filePath", Trim(parts(filePathIdx))
                    rec.Add "updatedAt", Trim(parts(updatedAtIdx))
                    If dict.Exists(code) Then
                        dict.Remove code
                    End If
                    dict.Add code, rec
                End If
            End If
        End If
    Loop
    ts.Close

    Set LoadManifest = dict
End Function

Sub SaveManifest(ByVal path, ByRef dict)
    Dim ts, code, rec
    On Error Resume Next
    Set ts = fso.OpenTextFile(path, ForWriting, True)
    If Err.Number <> 0 Then
        Err.Clear
        Exit Sub
    End If
    On Error GoTo 0

    ts.WriteLine "code,lastDate,lastClose,firstDate,firstClose,filePath,updatedAt"
    For Each code In dict.Keys
        Set rec = dict(code)
        ts.WriteLine CleanCsv(code) & "," & CleanCsv(GetRecValue(rec, "lastDate")) & "," & _
                     CleanCsv(GetRecValue(rec, "lastClose")) & "," & _
                     CleanCsv(GetRecValue(rec, "firstDate")) & "," & _
                     CleanCsv(GetRecValue(rec, "firstClose")) & "," & _
                     CleanCsv(GetRecValue(rec, "filePath")) & "," & _
                     CleanCsv(GetRecValue(rec, "updatedAt"))
    Next
    ts.Close
End Sub

Function GetOrCreateManifestRecord(ByRef dict, ByVal code)
    If dict.Exists(code) Then
        Set GetOrCreateManifestRecord = dict(code)
    Else
        Dim rec
        Set rec = CreateObject("Scripting.Dictionary")
        rec.Add "lastDate", ""
        rec.Add "lastClose", ""
        rec.Add "firstDate", ""
        rec.Add "firstClose", ""
        rec.Add "filePath", ""
        rec.Add "updatedAt", ""
        dict.Add code, rec
        Set GetOrCreateManifestRecord = rec
    End If
End Function

Function GetRecValue(ByRef rec, ByVal key)
    If rec.Exists(key) Then
        GetRecValue = rec(key)
    Else
        GetRecValue = ""
    End If
End Function

Function CleanCsv(ByVal value)
    Dim s
    s = CStr(value)
    s = Replace(s, ",", "_")
    s = Replace(s, vbCrLf, " ")
    s = Replace(s, vbLf, " ")
    CleanCsv = s
End Function

'============================================================
' Split detection
'============================================================
Function IsSplitSuspect(ByVal fileDate, ByVal fileClose, ByVal panDate, ByVal panClose, ByRef reason)
    reason = ""
    If Len(fileDate) = 0 Or Len(panDate) = 0 Then
        reason = "missing_date"
        IsSplitSuspect = True
        Exit Function
    End If
    If fileDate <> panDate Then
        reason = "date_mismatch"
        IsSplitSuspect = True
        Exit Function
    End If
    If panClose = 0 Then
        reason = "pan_close_zero"
        IsSplitSuspect = True
        Exit Function
    End If
    Dim diffAbs, diffRel
    diffAbs = Abs(ToDouble(fileClose) - ToDouble(panClose))
    diffRel = diffAbs / panClose
    If diffAbs >= ABS_DIFF_THRESHOLD Or diffRel >= ADJUST_DIFF_THRESHOLD Then
        reason = "close_diff"
        IsSplitSuspect = True
        Exit Function
    End If
    IsSplitSuspect = False
End Function

Sub AppendSplitSuspect(ByVal path, ByVal code, ByVal fileDate, ByVal fileClose, ByVal panDate, ByVal panClose, ByVal diffRel, ByVal reason)
    Dim ts, line
    If Not fso.FileExists(path) Then
        Set ts = fso.OpenTextFile(path, ForWriting, True)
        ts.WriteLine "code,fileDate,fileClose,panDate,panClose,diffRatio,reason,detectedAt"
        ts.Close
    End If
    Set ts = fso.OpenTextFile(path, ForAppending, True)
    line = CleanCsv(code) & "," & CleanCsv(fileDate) & "," & CleanCsv(fileClose) & "," & _
           CleanCsv(panDate) & "," & CleanCsv(panClose) & "," & CleanCsv(diffRel) & "," & _
           CleanCsv(reason) & "," & CleanCsv(FormatTimestamp(Now))
    ts.WriteLine line
    ts.Close
End Sub

'============================================================
' Fast read helpers
'============================================================
Function GetFirstRowInfo(ByVal filePath, ByRef firstDate, ByRef firstClose)
    Dim ts, line, parts
    GetFirstRowInfo = False
    firstDate = ""
    firstClose = ""

    On Error Resume Next
    Set ts = fso.OpenTextFile(filePath, ForReading, False)
    If Err.Number <> 0 Then
        Err.Clear
        Exit Function
    End If
    On Error GoTo 0

    Do While Not ts.AtEndOfStream
        line = Trim(ts.ReadLine)
        If Len(line) > 0 Then
            parts = Split(line, ",")
            If UBound(parts) >= 5 Then
                firstDate = Trim(parts(1))
                firstClose = Trim(parts(5))
                GetFirstRowInfo = True
                Exit Do
            End If
        End If
    Loop
    ts.Close
End Function

Function ReadLastRowInfoFast(ByVal filePath, ByRef lastDateStr, ByRef lastClose)
    Dim stream, size, startPos, bytes
    Dim textStream, text, lines, i, line, parts

    ReadLastRowInfoFast = False
    lastDateStr = ""
    lastClose = 0

    On Error Resume Next
    Set stream = CreateObject("ADODB.Stream")
    stream.Type = 1
    stream.Open
    stream.LoadFromFile filePath
    size = stream.Size
    If size <= 0 Then
        stream.Close
        Exit Function
    End If
    If size > TAIL_READ_BYTES Then
        startPos = size - TAIL_READ_BYTES
    Else
        startPos = 0
    End If
    stream.Position = startPos
    bytes = stream.Read(size - startPos)
    stream.Close

    Set textStream = CreateObject("ADODB.Stream")
    textStream.Type = 1
    textStream.Open
    textStream.Write bytes
    textStream.Position = 0
    textStream.Type = 2
    textStream.Charset = "us-ascii"
    text = textStream.ReadText
    textStream.Close
    On Error GoTo 0

    text = Replace(text, vbCrLf, vbLf)
    lines = Split(text, vbLf)
    For i = UBound(lines) To 0 Step -1
        line = Trim(lines(i))
        If Len(line) > 0 Then
            parts = Split(line, ",")
            If UBound(parts) >= 5 Then
                lastDateStr = Trim(parts(1))
                lastClose = ToDouble(parts(5))
                ReadLastRowInfoFast = True
                Exit Function
            End If
        End If
    Next
End Function

'============================================================
' Utils
'============================================================
Function FormatYYYYMMDD(ByVal d)
    FormatYYYYMMDD = Right("0000" & CStr(Year(d)), 4) & "/" & _
                     Right("0" & CStr(Month(d)), 2) & "/" & _
                     Right("0" & CStr(Day(d)), 2)
End Function

Function FormatTimestamp(ByVal d)
    FormatTimestamp = Right("0000" & CStr(Year(d)), 4) & "-" & _
                      Right("0" & CStr(Month(d)), 2) & "-" & _
                      Right("0" & CStr(Day(d)), 2) & " " & _
                      Right("0" & CStr(Hour(d)), 2) & ":" & _
                      Right("0" & CStr(Minute(d)), 2) & ":" & _
                      Right("0" & CStr(Second(d)), 2)
End Function

Function ToDouble(ByVal value)
    Dim s
    s = Trim(CStr(value))
    s = Replace(s, ",", "")
    On Error Resume Next
    Err.Clear
    ToDouble = CDbl(s)
    If Err.Number <> 0 Then
        Err.Clear
        ToDouble = 0
    End If
    On Error GoTo 0
End Function

Function ParseYYYYMMDDToDate(ByVal sDate)
    Dim parts, y, mm, dd
    ParseYYYYMMDDToDate = Empty
    sDate = Trim(sDate)
    If Len(sDate) = 0 Then Exit Function
    parts = Split(sDate, "/")
    If UBound(parts) <> 2 Then Exit Function
    On Error Resume Next
    Err.Clear
    y = CLng(parts(0))
    mm = CLng(parts(1))
    dd = CLng(parts(2))
    If Err.Number <> 0 Then
        Err.Clear
        Exit Function
    End If
    ParseYYYYMMDDToDate = DateSerial(y, mm, dd)
    If Err.Number <> 0 Then
        Err.Clear
        ParseYYYYMMDDToDate = Empty
    End If
    On Error GoTo 0
End Function

Function FindPosByDate(ByRef cal, ByVal beginPos, ByVal endPos, ByVal targetDate)
    Dim pos, d
    FindPosByDate = -1
    For pos = endPos To beginPos Step -1
        On Error Resume Next
        Err.Clear
        d = cal.Date(pos)
        If Err.Number = 0 Then
            If CDate(d) = CDate(targetDate) Then
                FindPosByDate = pos
                Exit Function
            End If
            If CDate(d) < CDate(targetDate) Then
                Exit For
            End If
        End If
        Err.Clear
        On Error GoTo 0
    Next
End Function

Function RemoveBOM(ByVal s)
    If Len(s) > 0 Then
        On Error Resume Next
        If AscW(Left(s, 1)) = &HFEFF Then
            RemoveBOM = Mid(s, 2)
        Else
            RemoveBOM = s
        End If
        On Error GoTo 0
    Else
        RemoveBOM = s
    End If
End Function

Function NormalizeCode(ByVal s)
    Dim x, a, i, ch, res
    x = Trim(s)
    If InStr(x, ",") > 0 Then
        a = Split(x, ",")
        x = Trim(a(0))
    End If
    If InStr(x, vbTab) > 0 Then
        a = Split(x, vbTab)
        x = Trim(a(0))
    End If
    If InStr(x, "_") > 0 Then
        a = Split(x, "_")
        x = Trim(a(0))
    End If
    If InStr(x, " ") > 0 Then
        a = Split(x, " ")
        x = Trim(a(0))
    End If
    res = ""
    For i = 1 To Len(x)
        ch = Mid(x, i, 1)
        If (ch >= "0" And ch <= "9") Or _
           (ch >= "A" And ch <= "Z") Or _
           (ch >= "a" And ch <= "z") Then
            res = res & ch
        Else
            Exit For
        End If
    Next
    NormalizeCode = UCase(res)
End Function

Function SanitizeFileName(ByVal s)
    Dim badChars, i
    s = Trim(s)
    If Len(s) = 0 Then
        SanitizeFileName = ""
        Exit Function
    End If
    badChars = Array(Chr(92), "/", ":", "*", "?", Chr(34), "<", ">", "|")
    For i = 0 To UBound(badChars)
        s = Replace(s, CStr(badChars(i)), "_")
    Next
    s = Trim(s)
    Do While Len(s) > 0 And (Right(s, 1) = "." Or Right(s, 1) = " ")
        s = Left(s, Len(s) - 1)
    Loop
    SanitizeFileName = s
End Function

Sub SafeOut(ByVal msg)
    On Error Resume Next
    WScript.StdOut.WriteLine msg
    If Err.Number <> 0 Then
        Err.Clear
        WScript.Echo msg
    End If
    On Error GoTo 0
End Sub

Function Q(ByVal s)
    Q = Chr(34) & Replace(CStr(s), Chr(34), Chr(34) & Chr(34)) & Chr(34)
End Function
