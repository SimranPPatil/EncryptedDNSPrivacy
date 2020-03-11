select "Total number of sites", count(distinct(site_url)) from ipprivacy.subsetting.`{table_analyzed}`;
select "Total number of resources", count(type) from ipprivacy.subsetting.`{table_analyzed}`;
select "Domains per site", count(*)/count(distinct(pageid)) from ipprivacy.subsetting.`{site_domains}`;
select "Distinct domains", count(distinct(load_domain)) from ipprivacy.subsetting.`{site_domains}`;
select type, count(*) from ipprivacy.subsetting.`{table_analyzed}` group by type;
select "Looked up domains", count(distinct(domain)) from ipprivacy.subsetting.`{domain2ip}`;
select "Distinct IPs", count(distinct(ip)) from ipprivacy.subsetting.`{domain2ip}`;
select "Average IPs per domain", avg(cnt) from (SELECT count(*) as cnt from ipprivacy.subsetting.`{domain2ip}` group by domain);
select "Average IP anonymity set", avg(cnt) from ipprivacy.subsetting.`{ip_anonsets}`;
select cnt,count(*) from ipprivacy.subsetting.`{ip_anonsets}` group by cnt order by cnt;
select "IPs with anonymity set 1",count(*) from ipprivacy.subsetting.`{ip_anonsets}` where cnt=1;
select "Average IP anonymity set for front page domains", avg(cnt) from ipprivacy.subsetting.`{site_ip_anonsets}`;
select cnt,count(*) from ipprivacy.subsetting.`{site_ip_anonsets}` group by cnt order by cnt;
select "IPs with anonymity set 1 for front pages",count(*) from ipprivacy.subsetting.`{site_ip_anonsets}` where cnt=1;