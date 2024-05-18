SELECT
    -- Select last verified user
    subquery.last_verified_user,
    -- Format created date to MM/DD/YYYY
    TO_CHAR(subquery.created_date::TIMESTAMP, 'MM/DD/YYYY') AS created_date,
    -- Construct property URL using property_id
    'https://example.com/property/' || subquery.property_id AS property_url,
    -- Construct listing URL using listing_id
    'https://example.com/listing/' || subquery.listing_id AS listing_url,
    -- Select various columns from the subquery
    subquery.type,
    subquery.usage,
    subquery.street_address,
    subquery.city,
    subquery.region,
    subquery.state,
    subquery.market_area,
    subquery.is_condominium,
    subquery.is_condo,
    subquery.land_area,
    subquery.building_area,
    subquery.total_floors,
    subquery.listing_floors,
    subquery.suite_number,
    subquery.available_area,
    subquery.max_contiguous_area,
    subquery.min_divisible_area,
    subquery.units_count,
    subquery.listing_type,
    subquery.availability_status,
    subquery.available_date,
    subquery.is_vacant,
    subquery.possession_type,
    subquery.list_price,
    subquery.min_rent,
    subquery.max_rent,
    -- Calculate min_rent_annualized based on rent_period and rent_size
    CASE
        WHEN subquery.rent_period = 'MONTHLY' THEN
            CASE
                WHEN subquery.rent_size = 'TOTAL' THEN
                    CASE
                        WHEN NULLIF(subquery.available_area, 0) IS NULL OR subquery.available_area < 1 THEN NULL
                        ELSE (subquery.min_rent * 12) / NULLIF(subquery.available_area, 0)
                    END
                ELSE subquery.min_rent * 12 -- Multiply by 12 for MONTHLY regardless of size
            END
        ELSE subquery.min_rent
    END AS min_rent_annualized,
    -- Apply similar logic for max_rent_annualized
    CASE
        WHEN subquery.rent_period = 'MONTHLY' THEN
            CASE
                WHEN subquery.rent_size = 'TOTAL' THEN
                    CASE
                        WHEN NULLIF(subquery.available_area, 0) IS NULL OR subquery.available_area < 1 THEN NULL
                        ELSE (subquery.max_rent * 12) / NULLIF(subquery.available_area, 0)
                    END
                ELSE subquery.max_rent * 12
            END
        ELSE subquery.max_rent
    END AS max_rent_annualized,
    subquery.rent_period,
    subquery.rent_size
FROM (
    -- Subquery to aggregate data and apply filters
    SELECT
        MAX(tbl1.verified_by) AS last_verified_user, -- Get the last verified user
        MAX(tbl1.date_created) AS created_date, -- Get the created date
        MAX(tbl1.property_id) AS property_id, -- Get the property id
        tbl1.listing_id AS listing_id, -- Get the listing id
        MAX(tbl2.type) AS type, -- Get the property type
        MAX(tbl1.usage) AS usage, -- Get the primary usage of the property
        MAX(tbl3.street_address) AS street_address, -- Get the street address
        MAX(tbl3.city) AS city, -- Get the city
        MAX(tbl3.region) AS region, -- Get the region
        MAX(tbl3.state) AS state, -- Get the state
        MAX(tbl4.market_area) AS market_area, -- Get the market area
        MAX(tbl3.is_condominium) AS is_condominium, -- Get the condominium status
        MAX(tbl1.is_condo) AS is_condo, -- Get the condo status
        MAX(tbl3.land_area) AS land_area, -- Get the land area
        MAX(tbl3.building_area) AS building_area, -- Get the building area
        MAX(tbl3.total_floors) AS total_floors, -- Get the total floors
        MAX(tbl1.listing_floors) AS listing_floors, -- Get the listing floors
        MAX(tbl1.suite_number) AS suite_number, -- Get the suite number
        MAX(tbl1.available_area) AS available_area, -- Get the available area
        MAX(tbl1.max_contiguous_area) AS max_contiguous_area, -- Get the maximum contiguous area
        MAX(tbl1.min_divisible_area) AS min_divisible_area, -- Get the minimum divisible area
        MAX(tbl1.units_count) AS units_count, -- Get the unit count
        MAX(tbl1.listing_type) AS listing_type, -- Get the listing type
        MAX(tbl1.availability_status) AS availability_status, -- Get the availability status
        MAX(tbl1.available_date) AS available_date, -- Get the available date
        MAX(tbl1.is_vacant) AS is_vacant, -- Get the vacant flag
        MAX(tbl1.possession_type) AS possession_type, -- Get the possession type
        MAX(tbl1.list_price) AS list_price, -- Get the list price
        AVG(tbl5.min_rent) AS min_rent, -- Get the average minimum rent
        AVG(tbl5.max_rent) AS max_rent, -- Get the average maximum rent
        MAX(tbl5.rent_period) AS rent_period, -- Get the rent period
        MAX(tbl5.rent_size) AS rent_size -- Get the rent size
    FROM main_table tbl1
    -- Join with table for additional property details
    LEFT JOIN property_table tbl3 ON tbl1.property_id = tbl3.property_id
    -- Join with rent table for rent details
    LEFT JOIN rent_table tbl5 ON tbl1.listing_key = tbl5.listing_key
    -- Join with market area table for RMA details
    LEFT JOIN market_table tbl4 ON tbl3.fips_code = tbl4.fips_code
    -- Join with type table for additional property type details
    LEFT JOIN type_table tbl2 ON tbl1.property_id = tbl2.property_id
    WHERE
        tbl1.date_created BETWEEN CURRENT_DATE - INTERVAL '7 days' AND CURRENT_DATE
        AND tbl1.availability_status = 'AVAILABLE' -- Only include available listings
        AND tbl1.listing_type = 'LEASE' -- Only include lease listings
        AND tbl4.market_area NOT IN ( -- Exclude specific RMAs
            'Region A',
            'Region B',
            'Region C',
            'Region D',
            'Region E',
            'Region F',
            'Region G',
            'Region H'
        )
    GROUP BY tbl1.listing_id -- Group by listing id
) AS subquery
ORDER BY subquery.market_area, subquery.street_address; -- Order results by market area and street address