/* ***********
Testing script for MySQL World Model procedures
   *********** */

/*
  Prepare the connection
*/

set collation_connection = utf16_unicode_ci;

/*
 Create some testing data
*/

call updateAttribute('test.uri','test.attribute1','test.script', 0x0101, 0);
call updateAttribute('test.uri','test.attribute1','test.script', 0x030A, 10);
call updateAttribute('test.uri','test.attribute1','test.script', 0x0514, 20);
call updateAttribute('test.uri','test.attribute1','test.script', 0x0732, 50);
call updateAttribute('test.uri','test.attribute1','test.script', 0x0928, 40);
call updateAttribute('test.uri','test.attribute1','test.script', 0x0B1E, 30);

call updateAttribute('test.uri','test.attribute2','test.script', 0x0205, 5);
call updateAttribute('test.uri','test.attribute2','test.script', 0x040F, 15);
call updateAttribute('test.uri','test.attribute2','test.script', 0x0619, 25);
call updateAttribute('test.uri','test.attribute2','test.script', 0x082D, 45);
call updateAttribute('test.uri','test.attribute2','test.script', 0x0A23, 35);

/*
Check searches
*/

call searchUri('test\.u.*');
call searchOrigin('test\.s[cr]{2}ipt');
call searchAttribute('te[Ss]t\.a[t]{2}ribute[12]');

/*
 Check range data
*/
call getRangeValues('test\.uri', null, 'test\.script', 0, 60); -- Return everything
call getRangeValues('test\.uri', null, 'test\.script', 0, 1); -- Only return attribute1
call getRangeValues('test\.uri', null, 'test\.script', 0, 0); -- Only return attribute1
call getRangeValues('test\.uri', null, 'test\.script', 0, 6); -- Only return attribute1/2 initial values
call getRangeValues('test\.uri', null, 'test\.script', 50, 60); -- Only return attribute1 final value

/*
  Check the current value - should be 0x0732
*/
call getCurrentValue('test\.uri', 'test\.attribute1', 'test\.script');
/*
  Should return 0x0732 (attribute1) and 0x082D (attribute2)
*/
call getCurrentValue('test\.uri', null, 'test\.script');

/* 
  Check some historic snapshots
*/
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 0); -- Should be 0x0732/0x082D
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 1); -- Should be 0x0105/NULL
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 46); -- Should be 0x0928/0x082D
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 51); -- Should be 0x0732/0x082D

/*
  Expire attribute2. No regexes allowed.
*/

call expireAttribute('test.uri', 'test.attribute2', 'test.script', 46);

/*
  Check the history
*/
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 0); -- Should be 0x0732/NULL
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 1); -- Should be 0x0105/NULL
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 46); -- Should be 0x0928/NULL
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 51); -- Should be 0x0732/NULL
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 26); -- Should be 0x0514/0x0619


/*
  Expire the Uri. No regexes here, so no worries about escaping characters
*/
call expireUri('test.uri', 'test.script', 55);

/*
  Check the current value - should be an empty set
*/
call getCurrentValue('test\.uri', 'test\.attribute[12]', 'test\.script');

/* 
  Check some historic snapshots (again/verify)
*/
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 0); -- Should be 0x0732/NULL
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 1); -- Should be 0x0105/NULL
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 46); -- Should be 0x0928/NULL
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 51); -- Should be 0x0732/NULL
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 26); -- Should be 0x0514/0x0619
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 55); -- Should be empty

/*
  Delete attribute1
*/
call deleteAttribute('test.uri', 'test.attribute1');

/* 
  Check some historic snapshots (again/verify)
*/
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 0); -- Should be 0x0732/NULL
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 1); -- Should be 0x0105/NULL
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 46); -- Should be 0x0928/NULL
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 51); -- Should be 0x0732/NULL
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 26); -- Should be 0x0514/NULL
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 55); -- Should be empty

/*
 Clean up everything else.
*/
call deleteUri('test.uri');

/* 
  Check some historic snapshots (again/verify).  Should all be empty.
*/
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 0); 
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 1); 
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 46); 
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 51); 
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 26); 
call getSnapshotValue('test\.uri', 'test\.attribute[12]', 'test\.script', 55); 
