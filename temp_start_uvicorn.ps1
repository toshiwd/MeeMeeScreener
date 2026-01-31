 = Start-Process -FilePath python -ArgumentList '-m','uvicorn','app.main:app','--host','127.0.0.1','--port','8000' -WorkingDirectory 'C:\work\meemee-screener' -NoNewWindow -PassThru
Write-Output .Id
