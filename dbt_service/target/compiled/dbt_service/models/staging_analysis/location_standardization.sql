-- Location standardization mapping
-- This model provides consistent state/region mapping to ISO 2-letter codes



with state_mapping as (
    select state_name, iso_code from (
        values
        ('California', 'CA'),
        ('Texas', 'TX'),
        ('Colorado', 'CO'),
        ('Georgia', 'GA'),
        ('New York', 'NY'),
        ('Illinois', 'IL'),
        ('Washington', 'WA'),
        ('Tennessee', 'TN'),
        ('Massachusetts', 'MA'),
        ('Florida', 'FL'),
        ('North Carolina', 'NC'),
        ('Virginia', 'VA'),
        ('Pennsylvania', 'PA'),
        ('Ohio', 'OH'),
        ('Michigan', 'MI'),
        ('Arizona', 'AZ'),
        ('Nevada', 'NV'),
        ('Oregon', 'OR'),
        ('Utah', 'UT'),
        ('Wisconsin', 'WI'),
        ('Minnesota', 'MN'),
        ('Maryland', 'MD'),
        ('Connecticut', 'CT'),
        ('New Jersey', 'NJ'),
        ('Indiana', 'IN'),
        ('Missouri', 'MO'),
        ('Alabama', 'AL'),
        ('Louisiana', 'LA'),
        ('Kentucky', 'KY'),
        ('South Carolina', 'SC'),
        ('Iowa', 'IA'),
        ('Arkansas', 'AR'),
        ('Kansas', 'KS'),
        ('Nebraska', 'NE'),
        ('Oklahoma', 'OK'),
        ('Mississippi', 'MS'),
        ('Delaware', 'DE'),
        ('Rhode Island', 'RI'),
        ('New Hampshire', 'NH'),
        ('Vermont', 'VT'),
        ('Maine', 'ME'),
        ('Montana', 'MT'),
        ('North Dakota', 'ND'),
        ('South Dakota', 'SD'),
        ('Wyoming', 'WY'),
        ('Idaho', 'ID'),
        ('Alaska', 'AK'),
        ('Hawaii', 'HI'),
        ('West Virginia', 'WV'),
        ('New Mexico', 'NM'),
        ('District of Columbia', 'DC'),
        ('Washington DC', 'DC'),
        ('Washington D.C.', 'DC')
    ) as t(state_name, iso_code)
),

standardization_rules as (
    select 
        state_name,
        iso_code,
        -- Create case-insensitive matching
        upper(state_name) as state_name_upper,
        upper(iso_code) as iso_code_upper
    from state_mapping
    
    union all
    
    -- Add entries where ISO codes map to themselves
    select 
        iso_code as state_name,
        iso_code,
        upper(iso_code) as state_name_upper,
        upper(iso_code) as iso_code_upper
    from state_mapping
)

select * from standardization_rules