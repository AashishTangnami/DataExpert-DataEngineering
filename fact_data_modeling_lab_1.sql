/*
	Fact : Something that happened or occured.
Eg: - A user logs in to an app
	- A transaction is made.

Fact data: Volume is enormous. It requires lot of context for effective analysis.
Duplicates in facts are way more common than in dimensional data.

\\
Normalization vs Denormalization

Normalized facts dont have any dimensional attributes. 
Denormalized facts bring in some dimensional attributes for qluicker analysis at the cost of nmore storage.

\\
Fact Data vs Raw Logs

Raw logs: 
	- ugly schemas desinged for online systems that make data analysis
	- potentially contains dupolicates and other quality errors.
	- Usually have shorter retention

Fact data:
	- Nice column names
	- Quality guarantees like uniquness.
*/
--------
/*
How does fact data modelling work?
	- Answers for Who, What, Where , When and How
*/