Clear-Host


$ScriptPath = split-path -parent $MyInvocation.MyCommand.Definition

& "$ScriptPath\ExportStatisticsChecksToExcel.ps1" -SQLInstance "AMORIM-7VQGKX3\SQL2022" -LogFilePath "C:\temp\StatsReview\" -Force_sp_GetStatisticInfo_Execution -CreateTranscriptLog -ShowVerboseMessages
