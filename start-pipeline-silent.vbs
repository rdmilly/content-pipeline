Set WshShell = CreateObject("Wscript.Shell")
Dim fso, envFile, line, parts
Set fso = CreateObject("Scripting.FileSystemObject")

' Load secrets from env file
envFile = "C:\Projects\Millyweb\content-pipeline\content-pipeline.env"
If fso.FileExists(envFile) Then
    Set f = fso.OpenTextFile(envFile, 1)
    Dim env
    Set env = WshShell.Environment("Process")
    Do Until f.AtEndOfStream
        line = Trim(f.ReadLine)
        If Len(line) > 0 And Left(line, 1) <> "#" Then
            pos = InStr(line, "=")
            If pos > 0 Then
                env(Left(line, pos - 1)) = Mid(line, pos + 1)
            End If
        End If
    Loop
    f.Close
End If

' Start transcriber (hidden)
WshShell.Run "python.exe C:\Projects\Millyweb\content-pipeline\transcriber\main.py", 0, False

' Start orchestrator (hidden)
WshShell.Run "python.exe C:\Projects\Millyweb\content-pipeline\orchestrator\main.py", 0, False
