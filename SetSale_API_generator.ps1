function start-apicall ($url,$call,$token) {
    [hashtable]$headers = @{
        "Authorization"="Bearer $token"
        "content-type"="application/json"
    }
    [hashtable]$rest_args = @{
        URI = $url
        Headers = $headers
    }

    if ($call) {
        $rest_args["Method"] = "POST"
        $rest_args["Body"] = $call
    } else {
        $rest_args["Method"] = "GET"
    }

    $tmp = invoke-restmethod @rest_args

    return $tmp
}

function initialize-modules ($module) {
    # Either Installs or imports DBATools to be used for SQL transactions
    if (!(get-module -Name $module)) {
        if (!(get-module -Name $module -ListAvailable)) {
            try {
                install-module ImportExcel
            } catch {
                write-host "Unable to install $module, exiting..."
                exit
            }
            write-host "$module installed successfully!"
        }
        try {
            import-module -Name $module
        } catch {
            write-host "Unable to import $module, exiting..."
            exit
        }
        write-host "$module imported successfully!"
    } else {
        write-host "$module already imported successfully!"
    }
}

function start-query {
    param (
        [Parameter(Mandatory=$true)]
        [string]$query,
        [Parameter(Mandatory=$true)]
        [string]$server,
        [Parameter(Mandatory=$true)]
        [string]$database,
        [Parameter(Mandatory=$true)]
        [PSCredential]$credentials
    )
    if (!$credentials) {$credentials = Get-Credential}
    $server = connect-dbainstance -SqlInstance $server -Database $database -SqlCredential $credentials
    if ($query -match "(Update|Delete|Truncate)") {
        return "No statements with Update/Delete/Truncate allowed"
    }
    $result = invoke-dbaquery -SqlInstance $server -Database $database -query $query
    Disconnect-DbaInstance $server
    return $result
}

function new-SQLQueryByBlock ($static,$expressions,$source_data,$blockSize) {
    [array]$queries = @()
    $total = $source_data.count
    $total_blocks = [math]::ceiling($total / $blockSize)
    $curr_block = 0

    new-spacer "-" 75
    write-host "Total amount of lines to process: $total`n"

    for ($i = 0; $i -lt $total; $i += $blockSize) {
        $endIndex = [math]::Min($i + $blockSize - 1, $total - 1)
        $block = $source_data[$i..$endIndex]
        $curr_block ++
        #$inner_count = 0
        new-spacer "-" 75
        write-host "Working on Block $curr_block out of $total_blocks"
        write-host "chunk $i through $endIndex..."
        [array]$queries += @"
$static $(
        (
            $block | foreach-object {
                #$inner_count ++
                #write-host "`t$inner_count/$($endIndex - $i)"
                $array = @()
                foreach ($x in $expressions.GetEnumerator()) {
                    set-variable -name $x.Name -value (invoke-expression $x.Value)
                    $tmp = (get-variable $x.Name).value
                    #write-host "$($x.name) set to $tmp using expression $($x.value)"
                    if (($tmp -notmatch "^\d+(\.\d+)?$" -or $x.name -eq "invoice_no" -or $x.name -eq "material") -and $tmp -ne "null") {$tmp = "'$tmp'"}
                    $array += $tmp
                }
                "($($array -join ","))"
            }
        ) -join ","
    )
"@
        write-host "Block completed succesfully!"
    }
    
    return $queries
}

function start-dataparse ($data) {
    $payloads = @()
    $grouped = $data | group-object customer_id
    foreach ($g in $grouped) {
        $payloads += [pscustomobject]@{
            "prices" = @(
                $g.Group | foreach-object {
                    [pscustomobject]@{
                        "contractorId" = $_.customer_id
                        "modelNumber" = $_.item_id
                        "price" = $_.unit_price
                    }
                }
            )
        }
    }

    return $payloads
}

function new-spacer ($symbol,$amount) {
    write-host ("`n" + $symbol * $amount + "`n")
    return
}

function start-main {
    param (
        [string]
        $server = "CWSQLEPICOR1",
        $database = "P21_live",
        $query = "select * from ydbc_api_customer_price_list order by customer_id",
        [int]
        $block = 950,
        [array]
        $errors = @(),
        [System.Collections.Specialized.OrderedDictionary]
        $insert_dict = [ordered]@{
            po_no = '$_.po_no'
            po_line_no = '$_.line_no'
            invoice_no = '$_.invoice_no'
            item_id = '$_.item_id'
            qty_shipped = '$_.qty_shipped'
            serials = '$_.serials -join ";"'
            imported_by = "return 'loh_carr_sn_import'"
        },
        [PSCredential]
        $sql_creds = (new-object system.management.automation.pscredential("carrierwest\MSSQLServ",(get-content "C:\temp\mssqlserv.txt" | convertto-securestring)))
        
    )

    new-spacer "*" 100

    # Either initializes or installs the required module
    initialize-modules "DBATools"

    new-spacer "*" 100

    # Grab data from YDBC pricing table for formatting
    $source_data = start-query -server $server -database $database -query $query -credentials $sql_creds

    # Generates payloads based on source data
    $payloads = start-dataparse $source_data

    # Iterate through the payloads, converts each one to json before delivering the payload to the necessary endpoint
    foreach ($payload in $payloads) {
        $json_payload = $payload | ConvertTo-Json
        try {
            start-apicall -url $base_url -token $token -call $json_payload
        } catch {
            write-host "An error occured..."
            $errors += [pscustomobject]@{
                msg = $_.Exception.Message
                payload = $payload
                timestamp = (get-date -format "yyyy-MM-dd HH:mm:ss")
            }
        }
    }

    return
}

# Sets variables to be used outside the scope of any functions
$ErrorActionPreference = [System.Management.Automation.ActionPreference]::Continue
#$log = "D:\logs\$((split-path -leaf $myinvocation.mycommand.definition).split(".")[0])_$(get-date -format "yyyyMMdd_HHmmss").log"
$log = "C:\temp\$((split-path -leaf $myinvocation.mycommand.definition).split(".")[0])_$(get-date -format "yyyyMMdd_HHmmss").log"

# Starts the transcript
start-transcript -force -includeinvocationheader -path $log

# Starts the main function
start-main

# Stops the transcript
stop-transcript

# Exits the program
exit
