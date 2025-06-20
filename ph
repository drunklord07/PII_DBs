#!/bin/bash
set -e

DB_USER="your_username"
DB_PASS="your_password"
DB_NAME="your_database"

MOBILE_REGEX='($$\\+91|\\+91-|91|0)?[6-9][0-9]{9}'

OUTPUT="mobile_numbers_extracted.csv"

	⁠"$OUTPUT"

TMPFILE=$(mktemp)

function log_error {
    echo "[ERROR] $1" >&2
    if [[ "$2" == "exit" ]]; then
        rm -f "$TMPFILE"
        exit 1
    fi
}

mapfile -t TABLES < <(
    mysql -u"$DB_USER" -p"$DB_PASS" -Nse "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '$DB_NAME';"
)

TABLE_COUNT=0

for TABLE in "${TABLES[@]}"; do
    echo "Scanning table: $TABLE"
    
    mapfile -t COLUMNS < <(
        mysql -u"$DB_USER" -p"$DB_PASS" -Nse "
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '$DB_NAME'
              AND TABLE_NAME = '$TABLE'
              AND DATA_TYPE IN ('char', 'varchar', 'text', 'longtext');
        "
    )

    for COL in "${COLUMNS[@]}"; do
        echo "  Scanning column: $COL"

        mysql -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" -Nse "SELECT \⁠ $COL\ ⁠ FROM \⁠ $TABLE\ ⁠ WHERE \⁠ $COL\ ⁠ REGEXP '$MOBILE_REGEX' LIMIT 5" > "$TMPFILE" 2>/dev/null
        PARA_EXIT_CODE=$?

        if [[ $PARA_EXIT_CODE -ne 0 ]]; then
            log_error "Failed to query column '$COL' in table '$TABLE'. MySQL error." continue
        fi

        # Extract mobile numbers from paragraph text
        grep -Eo "$MOBILE_REGEX" "$TMPFILE" | awk -v table="$TABLE" -v col="$COL" '{print table "," col "," $0}' | tee -a "$OUTPUT"
        
        # Direct match for mobile number format
        mysql -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" -Nse "
            SELECT '$TABLE', '$COL', \$COL
            FROM \$TABLE
            WHERE \$COL REGEXP '^$MOBILE_REGEX$'
            LIMIT 5;
        " 2>/dev/null | tee -a "$OUTPUT"
    done

    ((TABLE_COUNT++))
done

rm "$TMPFILE"

echo "Scan complete. Total tables scanned: $TABLE_COUNT"
echo "Extracted mobile numbers saved to: $OUTPUT"
