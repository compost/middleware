#!/usr/bin/env fish

set urls (cat applications.json | jq -r '"https://" + .host')

# Define color codes
set color_green (printf "\033[32m")  # Green color for RUNNING
set color_red (printf "\033[31m")    # Red color for errors
set color_reset (printf "\033[0m")   # Reset color

for url in $urls
    set response (curl --silent --connect-timeout 1  "$url/application/status")

    if test $(string match -r ".*RUNNING" "$response")
        echo -n "$url/application/status: $color_green$response$color_reset"
    else
        echo -n "$url/application/status: $response"
    end
    echo ""
end

