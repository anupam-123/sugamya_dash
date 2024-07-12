# GOS-DASH

### Prototype: Guesthouse/hotel Operating System Data Analytics System for Hospitality

**Project Responsibility**  
Gaurav S Hegde (grv.hegde@gmail.com)  
	Sugamya Academy for Excellence (SAfE)

**Overview**  
This system is part of a prototype in development at Sugamya/CodeSamarthya. It
has the following objectives:

- design and form a system for analysing customer and transactions data of a single
unit of a guesthouse, namely - Sugamya Corner unit of Sugamya Hospitality

- generate and provide reports that:  
	- support accounting & auditing 
	- present insights gained from analysing data with an objective of aiding decision making for business stakeholders

- integrate with WADO system: part of GOS

- prove instrumental in aiding design and development of product GOS

**Contributor(s)**  
- Anupam Neelavar (anupam2neelavar@gmail.com)  
	MCA, Pooja Bhagavat Memorial Mahajana Education Centre

- Gaurav S Hegde (grv.hegde@gmail.com)  
	Sugamya Academy for Excellence (SAfE)

---

### Execution Instructions
Here are instructions for running scripts and programs corresponding to 
the three Sub-Systems:

#### Data Preprocessing Sub-System (DPS)

#### Analytics & Forecasting Sub-System (AFS)

#### Visualization & Reporting Sub-System (VRS)


---

### For Developers & Contributors

Information related to the project and datasets for the code can be found in
[this notion page](https://www.notion.so/sugamya/Data-Analytics-Dashboarding-04859397eb2649a595b4893c74c523d6?pvs=4).
It is also the documentation page for this part of the prototype.
Information related to the overall system can be found in [this notion page](https://www.notion.so/sugamya/GOS-Guesthouse-Hotel-Operating-System-PoC-8a655d9b74654a3895b58d833d73612c?pvs=4).

*Note that appropriate permissions are necessary to access the above pages.*

#### During Development
If you wish to store datasets or some data for the purpose of code development,
please store in a directory titled 'datasets', this will be ignored by git.
**_NOTE: DO NOT INCLUDE DATA INTO REPOPSITORIES_**

#### Legend for Directories and Program Files

##### `dps_in`  
This directory is the default path directory where the script looks for
input data from the dataset. It consists of the following sub-directories:



##### `dps_out`  
This directory consists of the datasets after preprocessing for the other parts
of DASH. It consists of the following sub-directories:


##### `dataClean3.ipynb`  
This file contains unpolished code for cleaning the data. It is not used in the final code.


##### `dps_1_0.py`  

This file contains final polished code that follows the PEP8 style guide and it can be considered as a final code.

##### `dps_utils.py`

This module contains wrapper functions, converter functions and function that combine different catagory file to single dataframe
of different catagory.
