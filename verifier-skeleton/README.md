# Introduction

This project is intended to test your skill at manipulating and exploring data sets with complex semantics across a variety of storage technologies. Some parts of the project are underspecified or ambiguous. You are encouraged to ask questions throughout this process; please don't be shy.

# The story so far...

We're undertaking a large architectural refactoring to replace our legacy MySQL-based reporting technology with a system that pre-generates weekly reports for our customers and stores them as CSV files in Amazon S3. We expect this to improve the performance of our application and to save us the time and money it currently costs to keep the MySQL database running. Before we can decommission the legacy system, however, we need to move all of its data to the new reporting system.

# Goals

Your goal is to help us make sure that our migrator has successfully transferred all of our customers' data from MySQL to S3 without corrupting or dropping any records. We'll provide you a sample of migrated data from S3 and a sample of the source data from the relational database. You should verify (by extending the verifier application) that the data is consistent according to the rules described below.

The report you'll be verifying is about natural search rank. Each row - in both the legacy database table and the report in S3 - includes data about the positional rank of an URL within the search results for a particular keyword.

Where do you start?

This repository contains a partial implementation of a verification tool that we would like to be able to run to compare a report that's been migrated to S3 with the source data from our database. The central abstraction is an interface called _Verifier_. A complete _Verifier_ implementation _PrintingVerifier_ is included that does nothing but dump the contents of the S3 report object side-by-side with results of a query against the keyword\_url\_facts table in the database. Feel free to modify this implementation to explore the data.

There's also a class called _Application_ that will get you (fake) connections to S3 and the MySQL database and hook them up to the sample _Verifier_. Running the _main_ method in the _Application_ class will set everything up and run the verifier so you can see the output.

You'll notice that there's an empty _Verifier_ implementation called _ProductionVerifier_. It's your job to complete the implementation of this class so that it applies the rules described below!

# The schema of the S3 data warehouse

The data in the reports in S3 is in comma-separated value format. It has the following columns:

    keyword_id  link_type  url  domain_name  google_classic_rank  google_true_rank
    ----------  ---------  ---  -----------  -------------------  ----------------

Here are a few sample rows:

    838714,STANDARD_LINK,http://buybuybaby.com/202637156363777638,buybuybaby.com,1,1
    838714,STANDARD_LINK,http://albeebaby.com/4671006594144468821,albeebaby.com,2,2

You can interpret this data to mean that the URL http://buybuybaby.com/202637156363777638 (which belongs to buybuybaby.com) is ranking at position 1 for the keyword with id 838714, and the URL http://albeebaby.com/4671006594144468821 (from albeebaby.com) is ranking at position 2.

# The schema of the legacy MySQL database

Unlike the flattened data in S3, the data in the MySQL reporting database is normalized so that it spans several database tables.

## The domains table

    CREATE TABLE `domains` (
      `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
      `domain_name` varchar(256) COLLATE utf8_bin DEFAULT NULL,
      PRIMARY KEY (`id`),
    )

## The urls table

    CREATE TABLE `urls` (
      `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
      `domain_id` int(11) unsigned NOT NULL,
      `url` varchar(1000) COLLATE utf8_bin NOT NULL,
      PRIMARY KEY (`id`)
    )

## The keywords table

    CREATE TABLE `keywords` (
      `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
      `domain_id` int(11) unsigned NOT NULL,
      `keyword` varchar(255) COLLATE utf8_bin NOT NULL,
      PRIMARY KEY (`id`)
    )

## The keyword\_url\_facts table

    CREATE TABLE `keyword_url_facts` (
      `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
      `domain_id` int(11) unsigned NOT NULL,
      `url_id` int(11) unsigned NOT NULL,
      `keyword_id` int(11) unsigned NOT NULL,
      `google_search_rank` tinyint(3) unsigned DEFAULT NULL,
      `google_page_rank` tinyint(3) unsigned DEFAULT NULL,
      `bing_search_rank` tinyint(3) unsigned DEFAULT NULL,
      PRIMARY KEY (`id`)
    )

These tables have foreign key relationships such that you can join rows from the keyword\_url\_facts table with relevant rows from the associated dimensional tables. For example, you can produce a complete join using the following SQL query:

    SELECT * FROM keyword_url_facts
      JOIN domains ON domains.id = keyword_url_facts.domain_id
      JOIN urls ON urls.id = keyword_url_facts.url_id
      JOIN keywords ON keywords.id = keyword_url_facts.keyword_id;

# Verifying the data

Here are the characteristics of the report data that we'd like you to verify:

* Every row in the keyword\_url\_facts table in MySQL should have a corresponding row in the S3 report, except for cases in which the keyword\_url\_facts.google\_search\_rank and keyword\_url\_facts.google\_page\_rank columns are both NULL. In those cases, there should be no row in the S3 report.
* Every row in the S3 report should have a corresponding row in the keyword\_url\_facts table in MySQL.
* For a given keyword\_id in the S3 report, all google\_classic\_rank values should be unique and all google\_true\_rank values should be unique. (This is enforced by the database in our MySQL dataset.)
* The following identifiers are expected to match across reports:
    * keyword\_url\_facts.keyword_id and keywords.id in MySQL = keyword\_id in S3
    * domains.domain_name in MySQL = domain\_name in S3
    * urls.url in MySQL = url in S3
* The following data points are expected to match across reports:
    * keyword\_url\_facts.google\_search\_rank in MySQL = google\_classic\_rank in S3
    * keyword\_url\_facts.google\_page\_rank in MySQL = google\_true\_rank in S3
* The link\_type column in the S3 report should be set to "STANDARD\_LINK" for all rows
