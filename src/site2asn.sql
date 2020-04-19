select "Average ASN anonymity set for front page domains", avg(cnt) from ipprivacy.subsetting.`{site_asn_anonsets}`;
select cnt,count(*) from ipprivacy.subsetting.`{site_asn_anonsets}` group by cnt order by cnt;
select "ASNs with anonymity set 1 for front pages",count(*) from ipprivacy.subsetting.`{site_asn_anonsets}` where cnt=1;