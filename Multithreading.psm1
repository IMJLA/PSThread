function Split-Thread {
    <#
    .Synopsis
        Splits a command for a collection of input objects into multiple threads for asynchronous processing.
    
    .Description
        This script will allow any general, external script to be multithreaded by providing a single
        argument to that script and opening it in a seperate thread.  It works as a filter in the 
        pipeline, or as a standalone script.  It will read the argument either from the pipeline
        or from a filename provided.  It will send the results of the child script down the pipeline,
        so it is best to use a script that returns some sort of object.
    
        Based on original script by Ryan Witschger - http://www.Get-Blog.com - http://www.get-blog.com/?p=189
    
    .EXAMPLE
        Both of these will execute the script named ServerInfo.ps1 and provide each of the server names in AllServers.txt
        while providing the results to the screen.  The results will be the output of the child script.
        
        gc AllServers.txt | Split-Thread -Command .\ServerInfo.ps1
        Split-Thread -Command .\ServerInfo.ps1 -ObjectList (gc .\AllServers.txt)
    
    .EXAMPLE
        The following demonstrates the use of the AddParam statement
        
        $InputObject | Split-Thread -Command "Get-Service" -InputParam ComputerName -AddParam @{"Name" = "BITS"}
    
    .EXAMPLE
        The following demonstrates the use of the AddSwitch statement
        
        $InputObject | Split-Thread -Command "Get-Service" -AddSwitch @("RequiredServices", "DependentServices")
    
    .EXAMPLE
        The following demonstrates the use of the script in the pipeline
        
        $InputObject | Split-Thread -Command "Get-Service" -InputParam ComputerName -AddParam @{"Name" = "BITS"} | Select Status, MachineName 
    #>
    Param(

        # PowerShell Command or Script to run against each InputObject.
        [Parameter(Mandatory=$true)]
        $Command,

        <#
        Object to pass to the Command as an argument or parameter.
        This is an open ended argument and can take a single object from the pipeline, an array, a collection, or a file name.  The 
        multithreading script does it's best to find out which you have provided and handle it as such.  
        If you would like to provide a file, then the file is read with one object on each line and will 
        be provided as is to the script you are running as a string.  If this is not desired, then use an array.
        #>
        [Parameter(
            ValueFromPipeline=$true,
            ValueFromPipelineByPropertyName=$true
        )]
        $InputObject,

        # Named parameter of the Command to pass InputObject to
        $InputParameter = $Null,

        # Maximum number of concurrent threads to allow
        $Threads = 20,

        # Milliseconds to wait between cycles of the loop that checks threads for completion
        $SleepTimer = 200,

        # Seconds to wait without receiving any new results before giving up and stopping all remaining threads
        $Timeout = 120,

        <#
        This allows you to specify additional parameters to the running command.  For instance, if you are trying
        to find the status of the "BITS" service on all servers in your list, you will need to specify the "Name"
        parameter.  This command takes a hash pair formatted as follows: 
 
            @{"ParameterName" = "Value"}
            @{"ParameterName" = "Value" ; "ParameterTwo" = "Value2"}
        #>
        [HashTable]$AddParam = @{},

        <#
        This allows you to add additional switches to the command you are running.  For instance, you may want 
        to include "RequiredServices" to the "Get-Service" cmdlet.  This parameter will take a single string, or 
        an aray of strings as follows:

            "RequiredServices"
            @("RequiredServices", "DependentServices")
        #>
        [Array]$AddSwitch = @(),

        # Module names to pass to the Name parameter of Import-Module in every runspace.
        [String[]]$AddModule
    )
 
    $InitialSessionState = [system.management.automation.runspaces.initialsessionstate]::CreateDefault()
    
    <#
    Some modules (for example a module imported directly from a .psm1 file) will have a Definition property that contains all the module's code.
        We will run that Definition code inside the runspace.
    Other modules (for example the Active Directory module) have a null Definition property.
        In that case we will just try to import the module using Import-Module.
    #>

    $TempDir = "$Env:TEMP\PSMultithreading"
    $null = New-Item -ItemType Directory -Path $TempDir -ErrorAction SilentlyContinue

    $ModulesDir = "$TempDir\$((Get-Date -format s) -replace ':')"
    $null = New-Item -ItemType Directory -Path $ModulesDir -ErrorAction SilentlyContinue
    
    ForEach ($Module in $AddModule) {

        $ModuleObj = Get-Module $Module -ErrorAction SilentlyContinue

        if ($ModuleObj.Definition) {

            #Write-Debug "  $(Get-Date -Format s)`t$(hostname)`tSplit-Thread`tDefinition found for module '$Module'. Will import definition in each runspace."
                        
            $ModuleDir = "$ModulesDir\$($ModuleObj.Name)"
            $null = New-Item -ItemType Directory -Path $ModuleDir -ErrorAction SilentlyContinue

            $ModuleObj.Definition | Out-File -LiteralPath "$ModuleDir\$($ModuleObj.Name).psm1"

        }
        else {

            #Write-Debug "  $(Get-Date -Format s)`t$(hostname)`tSplit-Thread`tDefinition not loaded for module '$Module'. Will load module by name in each runspace."
            $InitialSessionState.ImportPSModule($Module)

        }

    }

    $InitialSessionState.ImportPSModulesFromPath($TempDir)

    $RunspacePool = [runspacefactory]::CreateRunspacePool(1, $Threads, $InitialSessionState, $Host)
    $RunspacePool.Open()

    $EnableOutputStreams = {
        $DebugPreference = 'Continue'
        $VerbosePreference = 'Continue'
        $WarningPreference = 'Continue'
        $ErrorActionPreference = 'Continue'
        $InformationPreference = 'Continue'
    }
     
    $AllThreads = Open-Thread -RunspacePool $RunspacePool -InputObject $InputObject
    $AllThreads = Wait-Thread -Thread $AllThreads -RunspacePool $RunspacePool -Threads $Threads -SleepTimer $SleepTimer -Timeout $Timeout -Dispose:$false
    $AllThreads = Invoke-Thread -Thread $AllThreads -Command $Command -InputParameter $InputParameter -AddParam $AddParam -AddSwitch $AddSwitch
    Wait-Thread -Thread $AllThreads -RunspacePool $RunspacePool -Threads $Threads -SleepTimer $SleepTimer -Timeout $Timeout

    $null = $RunspacePool.Close()
    $null = $RunspacePool.Dispose()
    Write-Progress -Activity 'Completed' -Completed

}

function Open-Thread {

    Param(

        <#
        Object to pass to the Command as an argument or parameter.
        This is an open ended argument and can take a single object from the pipeline, an array, a collection, or a file name.  The 
        multithreading script does it's best to find out which you have provided and handle it as such.  
        If you would like to provide a file, then the file is read with one object on each line and will 
        be provided as is to the script you are running as a string.  If this is not desired, then use an array.
        #>
        [Parameter(
            ValueFromPipeline=$true,
            ValueFromPipelineByPropertyName=$true
        )]
        $InputObject,

        # The .Net runspace pool to use for the threads
        [Parameter(
            Mandatory = $true
        )]
        $RunspacePool
    )
 
    begin {

        [int64]$CurrentObjectIndex = 0

    }
    process {

        ForEach ($Object in $InputObject){

            $CurrentObjectIndex++
            $ObjectString = $Object.ToString()
            #Write-Debug "  $(Get-Date -Format s)`t$(hostname)`tOpen-Thread`tLoading thread $CurrentObjectIndex : $ObjectString"
        
            $PowershellInterface = [powershell]::Create()
        
            $null= $PowershellInterface.AddScript($EnableOutputStreams)
        
            $PowershellInterface.RunspacePool = $RunspacePool

            $Handle = $PowershellInterface.BeginInvoke()
            
            $CurrentThread = [PSCustomObject]@{
                Handle = $Handle
                StopHandle = $null
                PowerShellInterface = $PowershellInterface
                Object = $Object
                ObjectString = $ObjectString
                Index = $CurrentObjectIndex
            }

            Write-Output $CurrentThread
        }

    }

    end {

    }
}

function Invoke-Thread {

    param (

        # Threads to start
        [Parameter(
            Mandatory = $true,
            ValueFromPipeline = $true
        )]
        [System.Collections.Generic.List[PSObject]]$Thread,

        # PowerShell Command or Script to run against each InputObject.
        [Parameter(Mandatory=$true)]
        $Command,

        # Named parameter of the Command to pass InputObject to
        $InputParameter = $Null,
        
        <#
        This allows you to specify additional parameters to the running command.  For instance, if you are trying
        to find the status of the "BITS" service on all servers in your list, you will need to specify the "Name"
        parameter.  This command takes a hash pair formatted as follows: 
 
            @{"ParameterName" = "Value"}
            @{"ParameterName" = "Value" ; "ParameterTwo" = "Value2"}
        #>
        [HashTable]$AddParam = @{},

        <#
        This allows you to add additional switches to the command you are running.  For instance, you may want 
        to include "RequiredServices" to the "Get-Service" cmdlet.  This parameter will take a single string, or 
        an aray of strings as follows:

            "RequiredServices"
            @("RequiredServices", "DependentServices")
        #>
        [Array]$AddSwitch = @()

    )
    begin {
        [int64]$CurrentObjectIndex = 0
    }
    process {

        ForEach ($CurrentThread in $Thread) {

            $CurrentObjectIndex++
            #Write-Debug "  $(Get-Date -Format s)`t$(hostname)`tInvoke-Thread`tStarting thread $CurrentObjectIndex : $($CurrentThread.ObjectString)"

            $null= $CurrentThread.PowershellInterface.Commands.Clear()
            $null= $CurrentThread.PowershellInterface.AddCommand($Command)

            If ($InputParameter -ne $Null){
                $null= $CurrentThread.PowershellInterface.AddParameter($InputParameter, $CurrentThread.Object)
                $InputParameterString = " -$InputParameter '$($CurrentThread.ObjectString)'"
            }
            Else{
                $null= $CurrentThread.PowershellInterface.AddArgument($CurrentThread.Object)
                $InputParameterString = " '$($CurrentThread.ObjectString)'"
            }

            $AdditionalParametersString = ForEach($Key in $AddParam.Keys){
                $null= $CurrentThread.PowershellInterface.AddParameter($Key, $AddParam.$key)
                Write-Output " -$Key '$($AddParam.$key)'"
            }
            
            $SwitchParameterString = ForEach($Switch in $AddSwitch){
                Write-Output " -$Switch"
                $null= $CurrentThread.PowershellInterface.AddParameter($Switch)
            }        

            Write-Verbose $("$(Get-Date -Format s)`t$(hostname)`tInvoke-Thread`t" + $Command + $InputParameterString + $AdditionalParametersString + $SwitchParameterString)

            $CurrentThread.Handle = $CurrentThread.PowershellInterface.BeginInvoke()

            Write-Output $CurrentThread

        }

    }
    end {}
}

function Wait-Thread {

    # TODO: Progress stream support

    param (

        # Threads to wait on
        [Parameter(
            Mandatory = $true,
            ValueFromPipeline = $true
        )]
        [PSObject[]]$Thread,

        [Parameter(
            Mandatory = $true
        )]
        $RunspacePool,
        
        # Maximum number of concurrent threads that are allowed (used only for progress display)
        $Threads = 20,
        
        # Milliseconds to wait between cycles of the loop that checks threads for completion
        $SleepTimer = 200,

        # Seconds to wait without receiving any new results before giving up and stopping all remaining threads
        $Timeout = 120,

        # Dispose of the thread when it is finished
        [switch]$Dispose = $true

    )

    $ResultTimer = Get-Date

    # Determine whether the threads have an Invoke in progress or a Stop in progress
    if ($Thread.StopHandle.IsCompleted) {
        [scriptblock]$Filter = {$_.StopHandle -ne $Null}
        [scriptblock]$CleanedUpFilter = {$_.StopHandle.IsCompleted -ne $false}
        [scriptblock]$CompletedFilter = {$_.StopHandle.IsCompleted -eq $true}
        [scriptblock]$IncompleteFilter = {$_.StopHandle.IsCompleted -eq $false}
    }
    else {
        [scriptblock]$Filter = {$_.Handle -ne $Null}
        [scriptblock]$CleanedUpFilter = {$_.Handle.IsCompleted -ne $false}
        [scriptblock]$CompletedFilter = {$_.Handle.IsCompleted -eq $true}
        [scriptblock]$IncompleteFilter = {$_.Handle.IsCompleted -eq $false}
    }

    While (@($Thread | Where-Object -FilterScript $Filter).count -gt 0)  {

        $CleanedUpThreads = @($Thread | Where-Object -FilterScript $CleanedUpFilter)
        $CompletedThreads = @($Thread | Where-Object -FilterScript $CompletedFilter)
        $IncompleteThreads = @($Thread | Where-Object -FilterScript $IncompleteFilter)
        $AvailableRunspaces = $RunspacePool.GetAvailableRunspaces()
        
        $ActiveThreadCountString = "$($Threads - $($AvailableRunspaces)) of $Threads are active"

        #Write-Debug "  $(Get-Date -Format s)`t$(hostname)`tWait-Thread`t$ActiveThreadCountString"
        #Write-Debug "  $(Get-Date -Format s)`t$(hostname)`tWait-Thread`t$($CleanedUpThreads.Count) completed threads"
        #Write-Debug "  $(Get-Date -Format s)`t$(hostname)`tWait-Thread`t$($IncompleteThreads.Count) incomplete threads"

        $RemainingString = "$($IncompleteThreads.Object)"
        If ($RemainingString.Length -gt 60){
            $RemainingString = $RemainingString.Substring(0,60) + "..."
        }

        $Progress = @{
            Activity = "Waiting on threads - $ActiveThreadCountString"
            PercentComplete = ($($CleanedUpThreads).count) / @($Thread).Count * 100
            Status = "$(@($IncompleteThreads).Count) remaining - $RemainingString"
        }
        Write-Progress @Progress
                 
        ForEach ($CompletedThread in $CompletedThreads) {

            #Write-Debug "  $(Get-Date -Format s)`t$(hostname)`tWait-Thread`tThread $($CompletedThread.Index) ($($CompletedThread.Object)) is complete"

            if ($CompletedThread.StopHandle.IsCompleted) {

                $null = $CompletedThread.PowershellInterface.EndStop($CompletedThread.StopHandle)
                $CompletedThread.StopHandle = $Null
                $Dispose = $true
                $ThreadOutput = $null

            }
            else {
                $ThreadOutput = $CompletedThread.PowerShellInterface.EndInvoke($CompletedThread.Handle)
            }

            if ($ThreadOutput.Count -gt 0) {
                #Write-Debug "  $(Get-Date -Format s)`t$(hostname)`tWait-Thread`tOutput (count of $($ThreadOutput.Count)) received from thread $($CompletedThread.Index) ($($CompletedThread.Object))"
            }
            else {
                #Write-Debug "  $(Get-Date -Format s)`t$(hostname)`tWait-Thread`tNull result for thread $($CompletedThread.Index) ($($CompletedThread.Object))"
            }

            #$CompletedThread.PowerShellInterface.Streams.Progress | ForEach-Object {Write-Progress $_}
            $CompletedThread.PowerShellInterface.Streams.Verbose | ForEach-Object {Write-Information $_}
            $CompletedThread.PowerShellInterface.Streams.Debug | ForEach-Object {Write-Information $_}
            $CompletedThread.PowerShellInterface.Streams.Warning | ForEach-Object {Write-Information $_}
            $CompletedThread.PowerShellInterface.Streams.Information | ForEach-Object {Write-Information $_}
            $null = $CompletedThread.PowerShellInterface.Streams.ClearStreams()

            $CompletedThread.Handle = $Null

            if ($Dispose -eq $true) {
                $null = $CompletedThread.PowerShellInterface.Dispose()
                $CompletedThread.PowerShellInterface = $Null
                Write-Output $ThreadOutput
            }
            else {
                Write-Output $CompletedThread
            }

            $ResultTimer = Get-Date

        }

        If (($(Get-Date) - $ResultTimer).totalseconds -gt $Timeout){

            Write-Warning "$(Get-Date -Format s)`t$(hostname)`tWait-Thread`tReached Timeout of $Timeout seconds. Killing $($IncompleteThreads.Count) remaining threads: $RemainingString"
            Stop-Thread -Thread $IncompleteThreads -AvailableRunspaces $AvailableRunspaces -Threads $Threads -RemainingString $RemainingString |
                Wait-Thread -RunspacePool $RunspacePool -Threads $Threads -SleepTimer $SleepTimer -Timeout $Timeout

        }

        Start-Sleep -Milliseconds $SleepTimer
        
    }
    
    Write-Progress -Activity 'Completed' -Completed  

}

function Stop-Thread {

    param (

        # Threads to start
        [Parameter(
            Mandatory = $true,
            ValueFromPipeline = $true
        )]
        [System.Collections.Generic.List[PSObject]]$Thread,

        $AvailableRunspaces,
        
        # Maximum number of concurrent threads that are allowed (used only for progress display)
        $Threads = 20,

        $RemainingString

    )
    begin{
                
        $CurrentThreadIndex = 0

    }
    process{

        ForEach ($CurrentThread in $Thread){

            $CurrentThreadIndex++
            Write-Verbose "$(Get-Date -Format s)`t$(hostname)`tStop-Thread`tStopping job $CurrentThreadIndex of $($Thread.Count) incomplete jobs."
            $RunningThreadCount = $Threads - $AvailableRunspaces

            $Progress = @{
                Activity = "Stopping Jobs - $RunningThreadCount of $Threads threads running" 
                PercentComplete = $CurrentThreadIndex / $Thread.Count * 100
                Status = "$(@($Thread).count - $CurrentThreadIndex + 1) remaining - $RemainingString" 
            }
            Write-Progress @Progress
                    
            Write-Verbose "$(Get-Date -Format s)`t$(hostname)`tStop-Thread`t[powershell]::BeginStop()"        
            $CurrentThread.StopHandle = $CurrentThread.PowershellInterface.BeginStop($null,$null)
            Write-Output $CurrentThread

        }
    }
    end{}
}

function Add-PowerShellCommand {

    param(

        [Parameter(ValueFromPipeline=$true)]
        $PowershellInterface,

        [Parameter(Position=0)]
        $Command

    )

    $CommandInfo = Get-Command $Command -ErrorAction SilentlyContinue
    Write-Debug "  $(Get-Date -Format s)`t$(hostname)`tAdd-PowerShellCommand`t$Command is a $($CommandInfo.CommandType)"

    switch ($CommandInfo.CommandType) {
        'Alias' {
            # Resolve the alias to its command and start from the beginning with that command.
            $PowershellInterface | Add-PowerShellCommand $CommandInfo.Definition
        }
        'Function' {
            $PowershellInterface.AddScript($CommandInfo.ScriptBlock)
        }
        'ExternalScript' {
        }
        default{
            # If the type is All, Application, Cmdlet, Configuration, Filter, or Script then run the command as-is
            $PowershellInterface.AddStatement().AddCommand($Command)
        }

    }

}
