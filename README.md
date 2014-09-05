# NAME

CohortExplorer - Explore clinical cohorts and search for entities of interest

# SYNOPSIS

**CohortExplorer \[OPTIONS\] COMMAND \[COMMAND-OPTIONS\]**

# DESCRIPTION

CohortExplorer provides an abstracted command line search interface for querying data and meta data stored in clinical data repositories implemented using the Entity-Attribute-Value (EAV) schema also known as the generic schema. Most of the available electronic data capture and clinical data management systems such as [LabKey](https://labkey.com/), [OpenClinica](https://www.openclinica.com/), [REDCap](http://project-redcap.org/) and [Opal](http://obiba.org/node/63) use EAV schema as it allows the organisation of heterogeneous data with relatively simple schema. With CohortExplorer's abstracted framework it is possible to 'plug-n-play' with clinical data repositories using the [datasource API](https://metacpan.org/pod/CohortExplorer::Datasource). The datasources stored in Opal and REDCap can be queried using the built-in APIs (see [here](http://www.youtube.com/watch?v=Tba9An9cWDY)).

The application makes use of the following concepts to explore clinical data repositories using the EAV schema:

- **Datasource**  

    A study or a cohort.

- **Standard datasource**

    Datasource which involves observing entities (e.g. the participant or drug) at a single time-point alone.

- **Longitudinal datasource** 

    Datasource which involves a repeated observation of entities over different time-points, visits or events.

- **Tables** 

    Questionnaires, surveys or forms in a datasource.

- **Variables and values** 

    The questions, which form part of the study, are called variables and values are answers to the questions.

- **Static table** 

    Questionnaires which are used only once in the course of the study are grouped under the static table. The static table represents data that is unchanging. All questionnaires within standard (or cross-sectional) datasources are static. However, the longitudinal datasources may also contain some questionnaires that can be grouped under the static table such as Demographics and FamilyHistory.

- **Dynamic table** 

    Questionnaires which are used repeatedly throughout the study are classed under the dynamic table. This table applies only to the longitudinal datasources and represents data that is changing with time.

# MOTIVATION

I have not found any query tools that can standardise the EAV schema. The EAV schema varies with electronic data capture and clinical data management systems. This poses a problem when two or more research groups collaborate on a project and the collaboration involves data exchange in the form of database dump (anonymised). The research groups may have used a different EAV schema to store clinical data. CohortExplorer is an attempt to standardise the entity attribute value model so the users can 'plug-n-play' with EAV schemas.

In addition, our group's specific query requirements also motivated me to write CohortExplorer.

# FEATURES

1. Allows the user to query datasources stored in multiple database instances.
2. Access to datasource is granted only after authentication.
3. Commands can be run on command line as well as interactively (i.e. console). It is easy to set-up a reporting system by adding commands to cron (under Linux).
4. Command-line completion for options and arguments wherever applicable.
5. Entities can be searched with/without imposing conditions on the variables.
6. Allows the user to save commands and use them to build new ones.
7. Datasource description including entity count can be obtained.
8. Allows the user to view summary statistics and export data on tables in csv format which can be readily parsed in statistical software like R for downstream analysis.
9. Allows the user to query for variables and view variable dictionary (i.e. meta data).

# OPTIONS

- **-d** _DATASOURCE_, **--datasource**=_DATASOURCE_

    Provide datasource

- **-u** _USERNAME_, **--username**=_USERNAME_

    Provide username

- **-p** _PASSWORD_, **--password**=_PASSWORD_

    Provide password

- **-h**, **--help**

    Show usage message and exit

- **-v**, **--verbose**

    Show with verbosity

**Note** the username and password is the same as the parent clinical data repository. The command is an argument to CohortExplorer.

# COMMANDS 

- **describe**

    This command outputs the datasource description in a tabular format where the first column is the table name followed by the table attributes such as label and variable\_count. The command also displays entity count for the specified datasource. For more on this command see [CohortExplorer::Command::Describe](https://metacpan.org/pod/CohortExplorer::Command::Describe/).

- **find**

    This command enables the user to find variables by supplying keywords. The command prints the dictionary of variables meeting the search criteria. The variable dictionary can include variable attributes such as variable name, table name, unit, categories (if any) and the associated label. For more on this command see [CohortExplorer::Command::Find](https://metacpan.org/pod/CohortExplorer::Command::Find).

- **search**

    This command enables the user to search for entities by supplying the variables of interest. The user can also impose conditions on the variables. For more on this command see [CohortExplorer::Command::Query::Search](https://metacpan.org/pod/CohortExplorer::Command::Query::Search).

- **compare**

    This command enables the user to compare entities across visits by supplying the variables of interest. The command is only available to the longitudinal datasources with data on at least 2 visits. For more on this command see [CohortExplorer::Command::Query::Compare](https://metacpan.org/pod/CohortExplorer::Command::Query::Compare). 

- **history**

    This command enables the user to see all previously saved commands. The user can utilise the existing information like options and arguments to build new commands. For more on this command see [CohortExplorer::Command::History](https://metacpan.org/pod/CohortExplorer::Command::History).

# EXAMPLES

    [somebody@somewhere]$ CohortExplorer --datasource=Medication --username=admin --password describe (run describe command)

    [somebody@somewhere]$ CohortExplorer -v -dMedication -uadmin -p sh (start console in verbose mode)
      
    [somebody@somewhere]$ CohortExplorer -dMedication -uadixit -p find -fi cancer diabetes (run find command with aliases)

# SECURITY

When setting CohortExplorer for group use it is recommended to install the application using its debian package which is part of the release. The package greatly simplifies the installation and implements the security mechanism. The security measures include:

- forcing the taint mode and,
- disabling the access to configuration files and log file to users other than the administrator or root (user).

To install type,

sudo dpkg -i cohortexplorer\_version-1\_all.deb

To install dependencies type,
sudo apt-get install -f

or install
sudo apt-get install gdebi

and then,
sudo gdebi cohortexplorer\_version-1\_all.deb

After the installation, the datasources can be configured by following the instructions in /etc/CohortExplorer/datasource-config.properties

To uninstall type,
sudo apt-get purge cohortexplorer

# BUGS 

Currently the application does not support the querying of datasources with multiple arms. The application is only tested with clinical data repositories implemented in MySQL and is yet to be tested with repositories implemented in Oracle, PostgreSQL and Microsoft SQL Server. Please report any bugs or feature requests to adixit@cpan.org.

# DEPENDENCIES

[Carp](https://metacpan.org/pod/Carp)

[CLI::Framework](https://metacpan.org/pod/CLI::Framework)

[Config::General](https://metacpan.org/pod/Config::General)

[DBI](https://metacpan.org/pod/DBI)

[Exception::Class::TryCatch](https://metacpan.org/pod/Exception::Class::TryCatch)

[File::HomeDir](https://metacpan.org/pod/HomeDir)

[File::Spec](https://metacpan.org/pod/File::Spec)

[Log::Log4perl](https://metacpan.org/pod/Log::Log4perl)

[SQL::Abstract::More](https://metacpan.org/pod/SQL::Abstract::More)

[Term::ReadKey](https://metacpan.org/pod/Term::ReadKey)

[Text::ASCIITable](https://metacpan.org/pod/Text::ASCIITable)

[Tie::IxHash](https://metacpan.org/pod/Tie::IxHash)

# ACKNOWLEDGEMENTS

Many thanks to the authors of all the dependencies used in writing CohortExplorer and also everyone for their suggestions and feedback.

# LICENSE AND COPYRIGHT

Copyright (c) 2013-2014 Abhishek Dixit (adixit@cpan.org). All rights reserved.

This program is free software: you can redistribute it and/or modify it under the terms of either:

- the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version, or
- the "Artistic Licence".

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details (http://www.gnu.org/licenses/).

On Debian systems, the complete text of the GNU General Public License can be found in '/usr/share/common-licenses/GPL-3'.

See http://dev.perl.org/licenses/ for more information.

# AUTHOR

Abhishek Dixit
