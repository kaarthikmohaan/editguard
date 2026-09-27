-- The 8 target wikis (design section 1). language_group splits the headline metric (English)
-- from the Indian-language wikis reported separately.
{# Rebuilt tables need a unique S3 location per build: dbt-athena swaps the new build in by rename. #}
{{ config(materialized='table', s3_data_naming='schema_table_unique') }}

select wiki_id, language, language_group
from (
    values
        ('enwiki', 'English', 'english'),
        ('bnwiki', 'Bengali', 'indian'),
        ('hiwiki', 'Hindi', 'indian'),
        ('knwiki', 'Kannada', 'indian'),
        ('mlwiki', 'Malayalam', 'indian'),
        ('mrwiki', 'Marathi', 'indian'),
        ('tawiki', 'Tamil', 'indian'),
        ('tewiki', 'Telugu', 'indian')
) as t (wiki_id, language, language_group)
