# JsonETL
Overall architecture

Input CSV dataset is processed with Json based transformations and loads the dataset into CSV. This process take the Json documents and parse the values for each fields and apply those transformations into input dataset. All the stages split into modular apprach for easy tesablity and resuablity purpose. Following are the modulues:

1. Input Reader
2. Json Parser
2. Custom transformations
3. Output Writer

Tool selection:

All the process been writen in spark scala modules

Error handling:

Used log4j for error handling on all the stages. 

Code structure :

The following modules:

1. sparkenv - Intitalise the Spark configuration 
2. methods - All the process happens on JsonRules and JsonTransform method
3. driver  - Driver script process all the stages Input Reader, Json Parser, Custom transformations and Output writer methods
4. utils  - All Json parser utils 

Executing Procedure:








