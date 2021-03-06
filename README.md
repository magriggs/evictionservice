# Eviction Service
A sample of how to implement an external eviction service.  

## ServerNode
Starts a server node with two example caches, DeclarationMaster and Declarations.  
There is a one-to-many relationship between DeclarationMaster and Declarations.  

The ServerNode either loads data from Ignite Persistent Store, or clears the old data and creates new example data
using a simple random algorithm.  Each declaration ID in DeclarationMaster will have between 500 and 1000 individual 
rows in the Declarations cache.

Usage: `ServerNode [--clear]`

### Arguments  
--clear: clears all previous data and re-creates with new example data. 


## EvictionClient
Starts a client node that takes an input parameter, rowNumberLimit.  If the number of rows in the Declarations cache exceeds rowNumberLimit, 
EvictionClient will begin to delete declarations.  It does this using the following process:
1.  `DeclarationMaster.remove(declarationId)`
2.  Finds all the rowIds that relate to the declarationId, using a SQL query
3.  Starts a DataStreamer on Declaration to remove all rows pertaining to the declarationId

There is **no special logic** to determine which declaration is removed.  The next declaration is returned 
by the function `getDeclarationIdToEvict`.  This method should be re-implemented to give the behaviour desired.

EvictionClient checks for excess rows once every 1000 milliseconds.  
