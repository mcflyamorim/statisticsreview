<#
    .SYNOPSIS
    Export SQL Server Statistics Checks to Excel
    .DESCRIPTION
    Export SQL Server Statistics Checks to Excel
    .NOTES
    .PARAMETER SqlInstance
    SQL Server instance name, use the full name, for instance, if server is ServerA and instance is SQL2005, use "ServerA\SQL2005"
    .PARAMETER UserName
    SQL Server LoginName to connect on SQL Server, if not specified WinAuth will be used.
    .PARAMETER Password
    SQL Server password to connect on SQL Server, if not specified WinAuth will be used.
    .PARAMETER Database
    Specify a specific Database, if not specified it will collect info about all online user DBs.
    .PARAMETER LogFilePath
    Path I'll use to create the "SQLServerStatisticsCheck_<>.xlsx" file with script output, default is $ScriptPath
    .LINK
    https://github.com/mcflyamorim
    .EXAMPLE
    Open a PowerShell console and run the following command:
    PS C:\>& "C:\temp\ExportStatisticsChecksToExcel\ExportStatisticsChecksToExcel.ps1" -SQLInstance "DELLFABIANO\SQL2019" -LogFilePath "C:\temp\" -Force_sp_GetStatisticInfo_Execution -CreateTranscriptLog
    .EXAMPLE
    Open a PowerShell console and run the following command:
    PS C:\>& "C:\temp\ExportStatisticsChecksToExcel.ps1" -SQLInstance "DELLFABIANO\SQL2019" -UserName "sa" -Password "@bc12345" -LogFilePath "C:\temp\" -Force_sp_GetStatisticInfo_Execution -CreateTranscriptLog
#>
param
(
    [parameter(Mandatory=$false)]
    [String] $SQLInstance = "DELLFABIANO\SQL2019",
    [String]$UserName,
    [String]$Password,
    [String]$Database = "",
    [parameter(Mandatory=$false)]
    [String] $LogFilePath = "C:\temp\",
    [parameter(Mandatory=$false)]
    [switch]$Force_sp_GetStatisticInfo_Execution = $false,
    [switch]$CreateTranscriptLog,
    [switch]$ShowVerboseMessages = $true
)
function Write-Msg {
    param (
        [string]$Message = '',
		[string]$Level = 'Output', # Output|Warning|Error
        [switch]$VerboseMsg
    )
    $ForegroundColor = switch ($Level) {
        'Output'  {'Cyan'}
        'Warning' {'Yellow'}
        'Error'   {'Red'}
        Default {'Cyan'}
    }
    $dt = Get-Date -format "yyyy-MM-dd hh:mm:ss"
    if (($ShowVerboseMessages) -and ($VerboseMsg)){
        Write-Host ("[{0}] - [$Level] - {1} `r" -f $dt, $Message) -ForegroundColor $ForegroundColor
    }
    elseif ($false -eq $VerboseMsg) {
        Write-Host ("[{0}] - [$Level] - {1} `r" -f $dt, $Message) -ForegroundColor $ForegroundColor
    }
}

function fnReturn {
    try {Stop-Transcript -ErrorAction SilentlyContinue | Out-Null} catch{}
    exit
}

function Add-ExcelImage {
    <#
    .SYNOPSIS
        Adds an image to a worksheet in an Excel package.
    .DESCRIPTION
        Adds an image to a worksheet in an Excel package using the
        `WorkSheet.Drawings.AddPicture(name, image)` method, and places the
        image at the location specified by the Row and Column parameters.
        
        Additional position adjustment can be made by providing RowOffset and
        ColumnOffset values in pixels.
    .EXAMPLE
        $image = [System.Drawing.Image]::FromFile($octocat)
        $xlpkg = $data | Export-Excel -Path $path -PassThru
        $xlpkg.Sheet1 | Add-ExcelImage -Image $image -Row 4 -Column 6 -ResizeCell
        
        Where $octocat is a path to an image file, and $data is a collection of
        data to be exported, and $path is the output path for the Excel document,
        Add-Excel places the image at row 4 and column 6, resizing the column
        and row as needed to fit the image.
    .INPUTS
        [OfficeOpenXml.ExcelWorksheet]
    .OUTPUTS
        None
    #>
    [CmdletBinding()]
    param(
        # Specifies the worksheet to add the image to.
        [Parameter(Mandatory, ValueFromPipeline)]
        [OfficeOpenXml.ExcelWorksheet]
        $WorkSheet,

        # Specifies the Image to be added to the worksheet.
        [Parameter(Mandatory)]
        [System.Drawing.Image]
        $Image,        

        # Specifies the row where the image will be placed. Rows are counted from 1.
        [Parameter(Mandatory)]
        [ValidateRange(1, [int]::MaxValue)]
        [int]
        $Row,

        # Specifies the column where the image will be placed. Columns are counted from 1.
        [Parameter(Mandatory)]
        [ValidateRange(1, [int]::MaxValue)]
        [int]
        $Column,

        # Specifies the name to associate with the image. Names must be unique per sheet.
        # Omit the name and a GUID will be used instead.
        [Parameter()]
        [string]
        $Name,

        # Specifies the number of pixels to offset the image on the Y-axis. A
        # positive number moves the image down by the specified number of pixels
        # from the top border of the cell.
        [Parameter()]
        [int]
        $RowOffset = 1,

        # Specifies the number of pixels to offset the image on the X-axis. A
        # positive number moves the image to the right by the specified number
        # of pixels from the left border of the cell.
        [Parameter()]
        [int]
        $ColumnOffset = 1,

        # Increase the column width and row height to fit the image if the current
        # dimensions are smaller than the image provided.
        [Parameter()]
        [switch]
        $ResizeCell
    )

    begin {
        if ($IsWindows -eq $false) {
            throw "This only works on Windows and won't run on $([environment]::OSVersion)"
        }
        
        <#
          These ratios work on my machine but it feels fragile. Need to better
          understand how row and column sizing works in Excel and what the
          width and height units represent.
        #>
        $widthFactor = 1 / 7
        $heightFactor = 3 / 4
    }

    process {
        if ([string]::IsNullOrWhiteSpace($Name)) {
            $Name = ([guid]::NewGuid()).ToString()
        }
        if ($null -ne $WorkSheet.Drawings[$Name]) {
            Write-Error "A picture with the name `"$Name`" already exists in worksheet $($WorkSheet.Name)."
            return
        }

        <#
          The row and column offsets of 1 ensures that the image lands just
          inside the gray cell borders at the top left.
        #>
        $picture = $WorkSheet.Drawings.AddPicture($Name, $Image)
        $picture.SetPosition($Row - 1, $RowOffset, $Column - 1, $ColumnOffset)
        
        if ($ResizeCell) {
            <#
              Adding 1 to the image height and width ensures that when the
              row and column are resized, the bottom right of the image lands
              just inside the gray cell borders at the bottom right.
            #>
            $width = $widthFactor * ($Image.Width + 1)
            $height = $heightFactor * ($Image.Height + 1)
            $WorkSheet.Column($Column).Width = [Math]::Max($width, $WorkSheet.Column($Column).Width)
            $WorkSheet.Row($Row).Height = [Math]::Max($height, $WorkSheet.Row($Row).Height)
        }
    }
}

Clear-Host
$CurrentDate = Get-Date

$global:ProgressPreference = 'Continue'

$ScriptPath = split-path -parent $MyInvocation.MyCommand.Definition
if ($CreateTranscriptLog){
    $TranscriptTimestamp = Get-Date -format "yyyyMMdd_HH_mm_ss_fff"
    Write-Msg -Message "Creating TranscriptLog on $ScriptPath\Log\StatisticCheck_LogOutput_$TranscriptTimestamp.txt" -VerboseMsg
    try {Stop-Transcript -ErrorAction SilentlyContinue | Out-Null} catch{}
    try {Start-Transcript -Path "$ScriptPath\Log\StatisticCheck_LogOutput_$TranscriptTimestamp.txt" -Force -ErrorAction | Out-Null} catch {Start-Transcript "$ScriptPath\Log\StatisticCheck_LogOutput_$TranscriptTimestamp.txt" | Out-Null}
}

# $User = [Security.Principal.WindowsIdentity]::GetCurrent()
# $Role = (New-Object Security.Principal.WindowsPrincipal $user).IsInRole([Security.Principal.WindowsBuiltinRole]::Administrator)
# if(!$Role) {
#     Write-Msg "Ops, to run this script you will need an elevated Windows PowerShell console..." -Level Error
#     fnReturn
# }

Write-Msg -Message "Starting script execution" -Level Warning

if (-Not $SQLInstance){
    Write-Msg "SQLInstance is a mandatory parameter" -Level Error
    fnReturn
}

# Check if $LogFilePath directory exists, if not, create it.
if ($LogFilePath -notlike '*\'){
    $LogFilePath = $LogFilePath + "\"
}
if(!(Test-Path $LogFilePath ))
{
    try {
        Write-Msg -Message "Creating directory: $LogFilePath" -VerboseMsg
        New-Item $LogFilePath -type Directory | Out-Null
    } catch {
        throw "Can't create $LogFilePath. You may need to Run as Administrator: $_"
        fnReturn
    }
}

# Check if $LogFilePathQueryPlans directory exists, if not, create it.
$LogFilePathQueryPlans = $LogFilePath + "QueryPlans_" + $CurrentDate.ToString("yyyyMMdd") + "_" + $CurrentDate.ToString("hhmm")
if(!(Test-Path $LogFilePathQueryPlans))
{
    try {
        Write-Msg -Message "Creating directory: $LogFilePathQueryPlans'" -VerboseMsg
        New-Item $LogFilePathQueryPlans -type Directory | Out-Null
    } catch {
        throw "Can't create $LogFilePathQueryPlans. You may need to Run as Administrator: $_"
        fnReturn
    }
}

$StatisticChecksFolderPath = "$ScriptPath\IndividualChecks\"
$instance = $SQLInstance
$SQLInstance = $SQLInstance.Replace('\','').Replace('/','').Replace(':','').Replace('*','').Replace('?','').Replace('"','').Replace('<','').Replace('>','').Replace('|','')
$FilePrefix = $SQLInstance + "_" + $CurrentDate.ToString("yyyyMMdd") + "_" + $CurrentDate.ToString("hhmm") + ".xlsx"
$FileOutput = $LogFilePath + "SQLServerStatisticsCheck_" + $FilePrefix

# Print input parameters values
Write-Msg -Message "------------------------------------------------------------------------" -VerboseMsg
Write-Msg -Message "Input parameters:" -VerboseMsg
Write-Msg -Message "SQLInstance: $instance" -VerboseMsg
Write-Msg -Message "UserName: $UserName" -VerboseMsg
Write-Msg -Message "Password: $Password" -VerboseMsg
Write-Msg -Message "Database: $Database" -VerboseMsg
Write-Msg -Message "LogFilePath: $LogFilePath" -VerboseMsg
Write-Msg -Message "Exporting data to $FileOutput" -VerboseMsg
Write-Msg -Message "Force_sp_GetStatisticInfo_Execution: $Force_sp_GetStatisticInfo_Execution" -VerboseMsg
Write-Msg -Message "CreateTranscriptLog: $CreateTranscriptLog" -VerboseMsg
Write-Msg -Message "ShowVerboseMessages: $ShowVerboseMessages" -VerboseMsg
Write-Msg -Message "------------------------------------------------------------------------" -VerboseMsg

# Installing ImportExcel module...
# Module may be installed but not imported into the PS scope session... if so, call import-module
if(Get-Module -Name ImportExcel -ListAvailable){
    Import-Module ImportExcel -Force -ErrorAction Stop
}
if(-not (Get-Module -Name ImportExcel))
{
    Write-Msg -Message "ImportExcel is not installed, trying to install" -VerboseMsg
    Write-Msg -Message "Trying to manually install ImportExcel from Util folder" -VerboseMsg
    if (Test-Path -Path "$ScriptPath\Util\ImportExcel\ImportExcel.zip" -PathType Leaf){
        try {
            foreach ($modpath in $($env:PSModulePath -split [IO.Path]::PathSeparator)) {
                #Grab the user's default home directory module path for later
                if ($modpath -like "*$([Environment]::UserName)*") {
                    $userpath = $modpath
                }
                try {
                    $temppath = Join-Path -Path $modpath -ChildPath "ImportExcel"
                    $localpath = (Get-ChildItem $temppath -ErrorAction Stop).FullName
                } catch {
                    $localpath = $null
                }
            }
            if ($null -eq $localpath) {
                if (!(Test-Path -Path $userpath)) {
                    try {
                        Write-Msg -Message "Creating directory: $userpath" -VerboseMsg
                        New-Item -Path $userpath -ItemType Directory | Out-Null
                    } catch {
                        throw "Can't create $userpath. You may need to Run as Administrator: $_"
                    }
                }
                # In case ImportExcel is not currently installed in any PSModulePath put it in the $userpath
                if (Test-Path -Path $userpath) {
                    $localpath = Join-Path -Path $userpath -ChildPath "ImportExcel"
                }
            } else {
                Write-Msg -Message "Updating current install" -VerboseMsg
            }
            $path = $localpath
            if (!(Test-Path -Path $path)) {
                try {
                    Write-Msg -Message "Creating directory: $path" -VerboseMsg
                    New-Item -Path $path -ItemType Directory | Out-Null
                } catch {
                    throw "Can't create $path. You may need to Run as Administrator: $_"
                }
            }

            $ImportExcelDir = "$path"
            $OutZip = Join-Path $ImportExcelDir 'ImportExcel.zip'
            Copy-Item -Path "$ScriptPath\Util\ImportExcel\ImportExcel.zip" -Destination $OutZip -ErrorAction Stop | Out-Null
            if (Test-Path $OutZip) {
                Write-Msg -Message "Trying to unzip $OutZip file" -VerboseMsg
                Add-Type -AssemblyName 'System.Io.Compression.FileSystem'
                [io.compression.zipfile]::ExtractToDirectory($OutZip, $ImportExcelDir)
                if (Test-Path "$ImportExcelDir\ImportExcel.psd1") {
                    Write-Msg -Message "File extracted to $ImportExcelDir" -VerboseMsg
                }
            }
            else {
                throw "$OutZip file was not found"
            }
            Import-Module $ImportExcelDir -Force -ErrorAction Stop
            Write-Msg -Message "ImportExcel installed successfully" -VerboseMsg
        } catch {
            Write-Msg -Message "Error trying to install ImportExcel from Util folder" -Level Error
            Write-Msg -Message "ErrorMessage: $($_.Exception.Message)" -Level Error
        }
    }
    else {
        Write-Msg "Could not find file $ScriptPath\Util\ImportExcel\ImportExcel.zip, please make sure you've copied ImportExcel.zip file to Util folder of this script." -Level Error
        fnReturn
    }
    if (-not (Get-Module -Name ImportExcel)) {
        try {
            Write-Msg -Message "Trying to install ImportExcel via Install-Module" -VerboseMsg
            Install-Module ImportExcel -Scope CurrentUser -Confirm:$False -Force -ErrorAction Stop | Out-Null
            Import-Module ImportExcel -Force -ErrorAction Stop
        } catch {
            Write-Msg -Message "Error trying to install ImportExcel via Install-Module" -Level Warning
            Write-Msg -Message "ErrorMessage: $($_.Exception.Message)" -Level Warning
        }
    }
}
if (-not (Get-Module -Name ImportExcel)) {
    Write-Msg -Message "ImportExcel is not installed, please install it before continue" -Level Error
    fnReturn
}

# Installing SqlServer module...
# Module may be installed but not imported into the PS scope session... if so, call import-module
if(Get-Module -Name SqlServer -ListAvailable){
    Import-Module SqlServer -Force -ErrorAction Stop
}
if(-not (Get-Module -Name SqlServer))
{
    Write-Msg -Message "SqlServer is not installed, trying to install" -VerboseMsg
    Write-Msg -Message "Trying to manually install SqlServer from Util folder" -VerboseMsg
    if (Test-Path -Path "$ScriptPath\Util\SqlServer\SqlServer.zip" -PathType Leaf){
        try {
            foreach ($modpath in $($env:PSModulePath -split [IO.Path]::PathSeparator)) {
                #Grab the user's default home directory module path for later
                if ($modpath -like "*$([Environment]::UserName)*") {
                    $userpath = $modpath
                }
                try {
                    $temppath = Join-Path -Path $modpath -ChildPath "SqlServer"
                    $localpath = (Get-ChildItem $temppath -ErrorAction Stop).FullName
                } catch {
                    $localpath = $null
                }
            }
            if ($null -eq $localpath) {
                if (!(Test-Path -Path $userpath)) {
                    try {
                        Write-Msg -Message "Creating directory: $userpath" -VerboseMsg
                        New-Item -Path $userpath -ItemType Directory | Out-Null
                    } catch {
                        throw "Can't create $userpath. You may need to Run as Administrator: $_"
                    }
                }
                # In case SqlServer is not currently installed in any PSModulePath put it in the $userpath
                if (Test-Path -Path $userpath) {
                    $localpath = Join-Path -Path $userpath -ChildPath "SqlServer"
                }
            } else {
                Write-Msg -Message "Updating current install" -VerboseMsg
            }
            $path = $localpath
            if (!(Test-Path -Path $path)) {
                try {
                    Write-Msg -Message "Creating directory: $path" -VerboseMsg
                    New-Item -Path $path -ItemType Directory | Out-Null
                } catch {
                    throw "Can't create $path. You may need to Run as Administrator: $_"
                }
            }

            $SqlServerDir = "$path"
            $OutZip = Join-Path $SqlServerDir 'SqlServer.zip'
            Copy-Item -Path "$ScriptPath\Util\SqlServer\SqlServer.zip" -Destination $OutZip -ErrorAction Stop | Out-Null
            if (Test-Path $OutZip) {
                Write-Msg -Message "Trying to unzip $OutZip file" -VerboseMsg
                Add-Type -AssemblyName 'System.Io.Compression.FileSystem'
                [io.compression.zipfile]::ExtractToDirectory($OutZip, $SqlServerDir)
                if (Test-Path "$SqlServerDir\SqlServer.psd1") {
                    Write-Msg -Message "File extracted to $SqlServerDir" -VerboseMsg
                }
            }
            else {
                throw "$OutZip file was not found"
            }
            Import-Module $SqlServerDir -Force -ErrorAction Stop
        } catch {
            Write-Msg -Message "Error trying to install SqlServer from Util folder" -Level Error
            Write-Msg -Message "ErrorMessage: $($_.Exception.Message)" -Level Error
        }
    }
    else {
        Write-Msg "Could not find file $ScriptPath\Util\SqlServer\SqlServer.zip, please make sure you've copied SqlServer.zip file to Util folder of this script." -Level Error
        fnReturn
    }
    if(-not (Get-Module -Name SqlServer)){
        try {
            Write-Msg -Message "Trying to install SqlServer via Install-Module" -VerboseMsg
            Install-Module SqlServer -Scope CurrentUser -Confirm:$False -Force -ErrorAction Stop | Out-Null
            Import-Module SqlServer -Force -ErrorAction Stop
        } catch {
            Write-Msg -Message "Error trying to install SqlServer via Install-Module" -Level Error
            Write-Msg -Message "ErrorMessage: $($_.Exception.Message)" -Level Error
        }
    }
}
if (-not (Get-Module -Name SqlServer)) {
    Write-Msg -Message "SqlServer is not installed, please install it before continue" -Level Error
    fnReturn
}

$Params = @{}
if ( $UserName -and $Password ) {
    $Params.Username = $UserName
    $Params.Password = $Password
}

try
{
    #If -Force_sp_GetStatisticInfo_Execution is set, recreate and run proc sp_GetStatisticInfo  
	if ($Force_sp_GetStatisticInfo_Execution) {
		Write-Msg -Message "Running proc sp_GetStatisticInfo, this may take a while to run, be patient."

        $TsqlFile = $StatisticChecksFolderPath + '0 - sp_GetStatisticInfo.sql'
		Invoke-SqlCmd @Params -ServerInstance $instance -Database "master" -MaxCharLength 10000000 -InputFile $TsqlFile -QueryTimeout 65535 <#18 hours#> -ErrorAction Stop

        #Using -Verbose to capture SQL Server message output
		if ($Database){
            $Query1 = "EXEC master.dbo.sp_GetStatisticInfo @database_name_filter = '$Database', @refreshdata = 1"
            Invoke-SqlCmd @Params -ServerInstance $instance -Database "master" -MaxCharLength 10000000 -QueryTimeout 65535 <#18 hours#> -Query $Query1 -Verbose -ErrorAction Stop
        }
        else{
            Invoke-SqlCmd @Params -ServerInstance $instance -Database "master" -MaxCharLength 10000000 -QueryTimeout 65535 <#18 hours#> -Query "EXEC master.dbo.sp_GetStatisticInfo @refreshdata = 1" -Verbose -ErrorAction Stop
        }
        
        Write-Msg -Message "Finished to run sp_GetStatisticInfo"
	}

    # CleanUp tables
    # $TsqlFile = $StatisticChecksFolderPath + '0 - CleanUp.sql'
	# Invoke-SqlCmd @Params -ServerInstance $instance -Database "tempdb" --InputFile $TsqlFile -ErrorAction Stop

    $TsqlFile = $StatisticChecksFolderPath + '0 - sp_CheckHistogramAccuracy.sql'
	Invoke-SqlCmd @Params -ServerInstance $instance -Database "master" -MaxCharLength 10000000 -QueryTimeout 65535 <#18 hours#> -InputFile $TsqlFile -ErrorAction Stop

	#Checking if tmp_stats table already exist
	$Result = Invoke-SqlCmd @Params -ServerInstance $instance -Database "tempdb" -Query "SELECT ISNULL(OBJECT_ID('tempdb.dbo.tmp_stats'),0) AS [ObjID]" -QueryTimeout 65535 <#18 hours#> -ErrorAction Stop | Select-Object -ExpandProperty ObjID

	if ($Result -eq 0) {
		Write-Msg "Could not find table tempdb.dbo.tmp_stats, make sure you've executed Proc sp_GetStatisticInfo to populate it." -Level Error
        Write-Msg "Use option -Force_sp_GetStatisticInfo_Execution to create and execute the proc" -Level Error
        fnReturn
	}

	# If result file already exists, remove it
    if (Test-Path $FileOutput) {
        Remove-Item $FileOutput -Force -ErrorAction Ignore
    }

    $files = get-childitem -path $StatisticChecksFolderPath -filter "Check*.sql" | Sort-Object { [regex]::Replace($_.Name, '\d+', { $args[0].Value.PadLeft(20) }) }

    foreach ($filename in $files)
    {
        $dt = Get-Date -Format 'yyyy-MM-dd hh:mm:ss'
	    [string]$str = "Running [" + ($filename.Name) + "]"
        Write-Msg -Message $str -Level Output

        try{
        	$Result = Invoke-SqlCmd @Params -ServerInstance $instance -Database "master" -MaxCharLength 10000000 -QueryTimeout 65535 <#18 hours#> -InputFile $filename.fullname -Verbose -ErrorAction Stop
        }
        catch 
        {
            Write-Msg -Message "Error trying to run the script. $filename" -Level Error
            $_.Exception.Message
            continue
        }
        $SecondsToRun = ((New-TimeSpan -Start $dt -End (Get-Date)).Seconds) + ((New-TimeSpan -Start $dt -End (Get-Date)).Minutes * 60)
		$dt = Get-Date -Format 'yyyy-MM-dd hh:mm:ss'
	    [string]$str = "Finished to run [" + ($filename.Name) + "], duration = " + $SecondsToRun.ToString()
        Write-Msg -Message $str -Level Output

		$dt = Get-Date -Format 'yyyy-MM-dd hh:mm:ss'
	    [string]$str = "Starting to write results on spreadsheet"
        Write-Msg -Message $str -Level Output

		if ($Result.count -eq 0){
			$Result = @([pscustomobject]@{Info="<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< ---------------------------------------------------------- no rows ---------------------------------------------------------- >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>";})
		}

        [int]$ResultRowCount = $Result.Length + 1;
		$filecontent = Get-Content -Path $filename.fullname

		$CheckDescription = @([pscustomobject]@{Info='Check details:'})
		foreach ($row in $filecontent) {
			if ($row.Trim() -eq '/*'){
				continue
			}			
			if ($row.Trim() -eq '*/'){
				break
			}
			$CheckDescription += @([pscustomobject]@{Info=$row.Trim()})
		}
		$NumberOfRowsDescription = $CheckDescription.count + 2

		$style = New-ExcelStyle -BackgroundColor LightYellow -Bold -Range "A1:D$NumberOfRowsDescription" -HorizontalAlignment Left -Merge `
								-VerticalAlignment Center -WrapText -BorderAround Thin

		$xl = $null
        $xl = $Result  | Select-Object * -ExcludeProperty "RowError", "RowState", "Table", "ItemArray", "HasErrors", "statement_plan" | `
                            Export-Excel -Path $FileOutput -WorkSheetname ($filename.Name).Replace('.sql', '') `
                                        -AutoSize -MaxAutoSizeRows 200 -AutoFilter -KillExcel -ClearSheet -TableStyle Medium2 `
                                        -Title ($filename.Name).Replace('.sql', '') -TitleBold <# -FreezePane 3 #> -TitleSize 20 `
                                        -StartRow ($NumberOfRowsDescription + 2) -Style $style -PassThru -Numberformat '#,##0'

		$SheetName = ($filename.Name).Replace('.sql', '')
		$ws = $xl.Workbook.Worksheets[$SheetName]
		$ws.Cells["A1"].Value = $CheckDescription | Select-Object -ExpandProperty Info | Out-String

		$ws.View.ZoomScale = 90
			
		$a = 65..90 | %{[char]$_}
        $a += 65..90 | %{'A' + [char]$_}
		$i = 0
        foreach ($c1 in $a) {
			$i = $i + 1
			$c2 = $c1 + '2' 
			$c2 = $c1 + ($NumberOfRowsDescription + 3).ToString()
			$ColValue = $ws.Cells["$c2"].Value

			if ($ColValue -like '*number_of_rows*'){
				$c2 = $c1 + ($NumberOfRowsDescription + 4).ToString()
				$c3 = $c1 + ($ResultRowCount + [int]($NumberOfRowsDescription + 3))
				$Range = $c2 + ':' + $c3 | Out-String
				Add-ConditionalFormatting -WorkSheet $ws -Address $Range -DataBarColor Blue
			}
			elseif ($ColValue -like '*modified_rows*') {
				$c2 = $c1 + ($NumberOfRowsDescription + 4).ToString()
				$c3 = $c1 + ($ResultRowCount + [int]($NumberOfRowsDescription + 3))
				$Range = $c2 + ':' + $c3 | Out-String
				Add-ConditionalFormatting -WorkSheet $ws -Address $Range -DataBarColor Red
			}
			elseif ($ColValue -like '*latch_since*') {
				$c2 = $c1 + ($NumberOfRowsDescription + 4).ToString()
				$c3 = $c1 + ($ResultRowCount + [int]($NumberOfRowsDescription + 3))
				$Range = $c2 + ':' + $c3 | Out-String
				Add-ConditionalFormatting -WorkSheet $ws -Address $Range -DataBarColor Red
			}
			elseif ($ColValue -like '*percent_of*') {
				$c2 = $c1 + ($NumberOfRowsDescription + 4).ToString()
				$c3 = $c1 + ($ResultRowCount + [int]($NumberOfRowsDescription + 3))
				$Range = $c2 + ':' + $c3 | Out-String
				Add-ConditionalFormatting -WorkSheet $ws -Address $Range -DataBarColor Green
			}
			elseif ($ColValue -like '*comment*') {
				$c2 = $c1 + ($NumberOfRowsDescription + 4).ToString()
				$c3 = $c1 + ($ResultRowCount + [int]($NumberOfRowsDescription + 3))
				$Range = $c2 + ':' + $c3 | Out-String
				Add-ConditionalFormatting -WorkSheet $ws -Range $Range -RuleType NotEqual `
											-ConditionValue 'OK' -ForeGroundColor "Red"
			}
            elseif ($ColValue -like '*datetime*') {
                $c2 = $c1 + ($NumberOfRowsDescription + 4).ToString()
				$c3 = $c1 + ($ResultRowCount + [int]($NumberOfRowsDescription + 3))
				$Range = $c2 + ':' + $c3 | Out-String
				$ws.Cells["$Range"].Style.Numberformat.Format = (Expand-NumberFormat -NumberFormat 'yyyy-mm-dd hh:mm:ss.fff')
            }
            elseif (($ColValue -like '*statement_plan*') -Or ($ColValue -like '*list_of_top_10_values_and_number_of_rows*') -Or ($ColValue -like '*statement_text*') -Or ($ColValue -like '*indexed_columns*') -Or ($ColValue -like '*index_list*') -Or ($ColValue -like '*stats_list*') -Or ($ColValue -like '*object_code_definition*') -Or ($ColValue -like '*referenced_columns*')) {
                Set-ExcelColumn -Worksheet $ws -Column $i -Width 30
            }
			elseif ($ColValue -eq $null) {
				break
			}
		}

		Close-ExcelPackage $xl #-Show
        $SecondsToRun = ((New-TimeSpan -Start $dt -End (Get-Date)).Seconds) + ((New-TimeSpan -Start $dt -End (Get-Date)).Minutes * 60)
		$dt = Get-Date -Format 'yyyy-MM-dd hh:mm:ss'
	    [string]$str = "Finished to write results on spreadsheet, duration = " + $SecondsToRun.ToString()
        Write-Msg -Message $str -Level Output

        try{
            if (($filename.Name) -like "*Plan cache usage*" ){
                $ResultRowCountCur = 1
                foreach ($row in $Result)
                {
                    $dt = Get-Date -Format 'yyyy-MM-dd hh:mm:ss'
                    
                    if (![string]::IsNullOrEmpty($row.statement_plan)) { 
                        $row.statement_plan | Format-Table -AutoSize -Property * | Out-String -Width 2147483647 | Out-File -FilePath "$LogFilePathQueryPlans\QueryPlan_$($row.query_hash).sqlplan" -Encoding unicode -Force
                    }
                    $SecondsToRun = ((New-TimeSpan -Start $dt -End (Get-Date)).Seconds) + ((New-TimeSpan -Start $dt -End (Get-Date)).Minutes * 60)
                    $dt = Get-Date -Format 'yyyy-MM-dd hh:mm:ss'
                    [string]$str = "Exporting plan cache info ($ResultRowCountCur of $($Result.Count)) - Finished to write $LogFilePathQueryPlans\QueryPlan_$($row.query_hash).sqlplan file , duration = " + $SecondsToRun.ToString()
                    Write-Msg $str
                    $ResultRowCountCur = $ResultRowCountCur + 1
                }
            }
        }
        catch 
        {
            Write-Msg -Message "Error trying to export file." -Level Error
            Write-Msg -Message "ErrorMessage: $($_.Exception.Message)" -Level Error
            continue
        }        
    }

	try{
		$SummaryTsqlFile = $StatisticChecksFolderPath + '0 - Summary.sql'
		$Result = Invoke-SqlCmd @Params -ServerInstance $instance -Database "master" -MaxCharLength 10000000 -QueryTimeout 65535 <#18 hours#> -InputFile $SummaryTsqlFile -ErrorAction Stop
		$ResultChart1 = Invoke-SqlCmd @Params -ServerInstance $instance -Database "master" -MaxCharLength 10000000 -QueryTimeout 65535 <#18 hours#> `
                            -Query "SELECT prioritycol, COUNT(*) AS cnt FROM tempdb.dbo.tmpStatisticCheckSummary WHERE prioritycol <> 'NA' GROUP BY prioritycol" `
                            -ErrorAction Stop
	}
	catch 
	{
		Write-Msg -Message "Error trying to run the script. $SummaryTsqlFile"
		$_.Exception.Message
		continue
	}

    $CustomColor = [System.Drawing.Color]::FromArgb("32","55", "100")
    $style = New-ExcelStyle -Range "A1:D1" -HorizontalAlignment Left -Merge -VerticalAlignment Center -WrapText `
                            -BackgroundColor $CustomColor

	$xlPkg = $Result | Select-Object * -ExcludeProperty  "RowError", "RowState", "Table", "ItemArray", "HasErrors" | `
						Export-Excel -Path $FileOutput -WorkSheetname 'Summary' `
									-AutoSize -MaxAutoSizeRows 200 -KillExcel -ClearSheet -TableStyle Medium2 `
									-TableName "Summary0" -Style $style `
									-StartRow 8 -MoveToStart -PassThru -Numberformat '#,##0'

    $ReportLogo = "$ScriptPath/ReportLogo.png"
    $image = [System.Drawing.Image]::FromFile($ReportLogo)
    $xlpkg.Summary | Add-ExcelImage -Image $image -Row 1 -Column 5 -ResizeCell
    
	#$lastRow = $xl.Workbook.Worksheets['Summary'].Dimension.End.Row + 1
	$chartDef = New-ExcelChartDefinition -ChartType Pie -Title "" -XRange "Summary1[prioritycol]" -YRange "Summary1[cnt]" `
								-Width 225 -Height 100 -Row 1 -Column 4 -LegendPosition Right
							
	$xlPkg = $ResultChart1 | Select-Object * -ExcludeProperty  "RowError", "RowState", "Table", "ItemArray", "HasErrors" | `
                                Export-Excel -ExcelPackage $xlPkg -WorkSheetname 'Summary' `
                                            -TableStyle Medium2 -AutoSize `
                                            -TableName "Summary1" -TitleBold -TitleSize 12 -MoveToStart `
                                            -StartRow 3 -StartColumn 2 `
                                            -ExcelChartDefinition $chartDef -PassThru -Numberformat '#,##0'
	
	#$xl = Open-ExcelPackage $FileOutput -KillExcel
	$ws = $xlPkg.Workbook.Worksheets['Summary']
    $ws.View.ShowGridLines = $false

    $ws.Cells["A1"].Value = 'SQL Server statistics review'
    Set-ExcelRange -Address $ws.Cells["A1"] -Bold -HorizontalAlignment Center -FontSize 24 -FontColor ([System.Drawing.Color]::White)

    $ws.Cells["A3"].Value = "Summary"
    Set-ExcelRange -Address $ws.Cells["A3"] -Bold -HorizontalAlignment Left -FontSize 14 -FontColor $CustomColor
    
    $ws.Cells["A4"].Value = "SQL Server Instance: " + $instance
    $ws.Cells["A5"].Value = "Number of checks: " + $files.Count.ToString()
    $ws.Cells["A6"].Value = "Report generated: " + $CurrentDate.ToString("yyyy-MM-dd") + ' ' + $CurrentDate.ToString("hh:mm")
    
    $startRow = 1
    $endRow   = $ws.Dimension.End.Row    
    while ($startRow -le $endRow) {
        $Display = $ws.Cells[$StartRow, 4].Value

        foreach ($item in $ws.Workbook.Worksheets) {
            if ($item.Name -like "$Display-*"){
                $startRowSpreadSheet = 1
                $LinkRow = 1
                $endRowSpreadSheet = $item.Dimension.End.Row
                while ($startRowSpreadSheet -le $endRowSpreadSheet) {
                    $RowValue = $item.Cells[$startRowSpreadSheet, 1].Value
                    if ($RowValue -eq $item.Name){
                        $LinkRow = $startRowSpreadSheet
                        break
                    }
                    $startRowSpreadSheet++
                }
                $SpreadSheetName = $item.Name
                $HyperLinkText = "'" + $SpreadSheetName + "'!A$LinkRow"
                $HyperLink = New-Object -TypeName OfficeOpenXml.ExcelHyperLink -ArgumentList $HyperLinkText, $Display
                $ws.Cells[$StartRow, 4].Hyperlink = $HyperLink
                $ws.Cells[$StartRow, 4].Style.Font.UnderLine = $true
                $ws.Cells[$StartRow, 4].Style.Font.Color.SetColor([System.Drawing.Color]::Blue)
            }
        }
        $startRow++
    }

	$r = Add-ConditionalFormatting -Worksheet $ws -Range "B:B"-ThreeIconsSet Symbols -Passthru
    $r.Reverse = $true;
	$r.Icon2.Type = "Num" 
	$r.Icon3.Type = "Num"
	$r.Icon1.Value = 0
	$r.Icon2.Value = 1
	$r.Icon3.Value = 1
	$ws.View.ZoomScale = 90

	Add-ConditionalFormatting -WorkSheet $ws -Range "C4:C6" -DataBarColor Red
	
	Close-ExcelPackage $xlPkg #-Show
}
catch 
{
    Write-Msg -Message "Error trying to run the script." -Level Error
    Write-Msg -Message "ErrorMessage: $($_.Exception.Message)" -Level Error
    fnReturn
}

Write-Msg -Message "Finished script execution" -Level Warning
try {Stop-Transcript -ErrorAction SilentlyContinue | Out-Null} catch{}