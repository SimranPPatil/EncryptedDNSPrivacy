select "Average IP anonymity set for front page domains", avg(cnt) from ipprivacy.subsetting.`{site_ip_anonsets}`;
select cnt,count(*) from ipprivacy.subsetting.`{site_ip_anonsets}` group by cnt order by cnt;
select "IPs with anonymity set 1 for front pages",count(*) from ipprivacy.subsetting.`{site_ip_anonsets}` where cnt=1;