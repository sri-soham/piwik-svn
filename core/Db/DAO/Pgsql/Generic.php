<?php
/**
 * Piwik - Open source web analytics
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 * @category Piwik
 * @package Piwik
 */

/**
 * @package Piwik
 * @subpackage Piwik_Db
  */
class Piwik_Db_DAO_Pgsql_Generic
{
	private $db;

	private $isByteaOutputHex;

	private $hexEscapeLookup;

	public function __construct($db)
	{
		$this->db = $db;
		$this->isByteaOutputHex = null;
	}

	public function getSqlRevenue($field)
	{
		return "ROUND(".$field."::numeric, ".Piwik_Tracker_GoalManager::REVENUE_PRECISION.")";
	}

	// $where = array of the form
	// array(' column1 = ? ', ' AND ', 'colum2 <= ? ', ' OR ', ' column3 > ?')
	// Number of elements of where will always be odd. If there is more than
	// one column in the where clause, first element will be the first condition
	// second element will be the logical operator (AND or OR)
	// third element will be the next condition
	// and so on.
	public function deleteAll($table, $where, $maxRowsPerQuery, $parameters=array())
	{
		if (empty($where))
		{
			throw new Exception('This function will work only when there is a WHERE clause');
		}
		if ((count($where) % 2) == 0)
		{
			throw new Exception('Invalid where array. Number of elements should be odd');
		}
		$columns = $this->getColumnsFromWhere($where);

		$sql = 'DELETE FROM ' . $table . ' WHERE (' . $columns . ') '
			 . 'IN ((SELECT ' . $columns . ' FROM ' . $table . ' '
			 . '    WHERE ' . implode(' ', $where) . ' LIMIT ' . (int)$maxRowsPerQuery
			 . '    ))';

		$totalRowsDeleted = 0;
		do 
		{
			$rowsDeleted = $this->db->query($sql, $parameters)->rowCount();
			$totalRowsDeleted += $rowsDeleted;
		} while ($rowsDeleted >= $maxRowsPerQuery);

		return $totalRowsDeleted;
	}

	public function hasBlobDataType()
	{
		return $this->db->hasBlobDataType();
	}

	public function beginTransaction()
	{
		$sql = 'START TRANSACTION';
		$this->db->query($sql);
	}

	public function commit()
	{
		$sql = 'COMMIT';
		$this->db->query($sql);
	}

	public function rollback()
	{
		$sql = 'ROLLBACK;';
		$this->db->query($sql);
	}

	public function setTimeout($millis=1000)
	{
		$this->db->query('SET statement_timeout TO ' . $millis);
	}

	public function resetTimeout()
	{
		$this->db->query('RESET statement_timeout');
	}

	/**
	 * Tries to get lock on table with a NOWAIT clause. The NOWAIT clause
	 * will cause the other LOCK TABLE requests to fail immediately.
	 * This will begin the transaction implicitly. If lock could not be acquired
	 * then transaction should be rolled back for other queries to execute.
	 * @param string $table Name of the table
	 * @param string $lockType Type of the lock; defaults to ACCESS EXCLUSIVE
	 * @param integer $maxRetries The number of times to retry
	 * @return bool success/failure in obtaining the lock
	 */
	public function getTableLock($table, $lockType = 'ACCESS EXCLUSIVE', $maxRetries = 30)
	{
		$sql = 'LOCK TABLE ' . $table . ' IN ' . $lockType . ' MODE ';
		while ($maxRetries > 0)
		{
			$this->beginTransaction();
			$this->setTimeout();
			if (@$this->db->query($sql))
			{
				$this->resetTimeout();
				return true;
			}
			else
			{
				$this->rollback(); // without this other queries will not be executed
			}
			--$maxRetries;
		}

		$this->resetTimeout();
		return false;
	}

	public function lockTables($tablesToRead, $tablesToWrite)
	{
		if (!is_array($tablesToRead))
		{
			$tablesToRead = array($tablesToRead);
		}
		if (!is_array($tablesToWrite))
		{
			$tablesToWrite = array($tablesToWrite);
		}

		$sql = 'LOCK TABLES ' . implode(', ', $tablesToRead) . ' IN ACCESS SHARE MODE';
		$this->db->exec($sql);

		$sql = 'LOCK TABLES ' . implode(', ', $tablesToWrite) . ' IN ACCESS EXCLUSIVE MODE';
		$this->db->exec($sql);
	}

	public function insertIgnore($sql, $bind)
	{
		try {
			$this->db->query($sql, $bind);
			$result = true;
		}
		catch(Exception $e) {
			// postgresql code error 23505: unique_violation
			if(!$this->db->isErrNo($e, '23505'))
			{
				throw $e;
			}
			$result = false;
		}

		return $result;
	}

	/**
	 * Attempts to get a named lock.
	 * 
	 * @param string $lockName
	 * @return bool true if the lock was obtained, false if otherwise.
	 */
	public function getDbLock( $lockName, $maxRetries=null)
	{
		$sql = 'SELECT pg_advisory_lock(?)';

		$rows = $this->db->fetchAll($sql, array($lockName));
		return count($rows) == 1;
	}
	
	/**
	 * Releases a named lock.
	 * 
	 * @param string $lockName The lock name.
	 * @return bool true if the lock was released, false if otherwise.
	 */
	public function releaseDbLock( $lockName )
	{
		$sql = 'SELECT pg_advisory_unlock(?)';

		return $this->db->fetchOne($sql, array($lockName)) == 't';
	}

	/**
	 * Performs a batch insert into a specific table by iterating through the data
	 *
	 * NOTE: you should use tableInsertBatch() which will fallback to this function if LOAD DATA INFILE not available
	 *
	 * @param string  $tableName            PREFIXED table name! you must call Piwik_Common::prefixTable() before passing the table name
	 * @param array   $fields               array of unquoted field names
	 * @param array   $values               array of data to be inserted
	 * @param bool    $ignoreWhenDuplicate  Ignore new rows that contain unique key values that duplicate old rows
	 */
	public function insertIgnoreBatch($tableName, $fields, $values, $ignoreWhenDuplicate=true)
	{
		$fieldList = '('.join(',', $fields).')';
		$params = Piwik_Common::getSqlStringFieldsArray($values[0]);
		$sql = 'INSERT INTO ' . $tableName . $fieldList . ' VALUES (' . $params . ')';
		
		if ($ignoreWhenDuplicate)
		{
			foreach ($values as $row)
			{
				$this->insertIgnore($sql, $row);
			}
		}
		else 
		{
			foreach ($values as $row)
			{
				$this->db->query($sql, $row);
			}
		}
	}

	public function hour($timestampCol)
	{
		return ' EXTRACT (HOUR FROM ' . $timestampCol . ') ';
	}

	/**
	 *	casts numeric strings to numeric
	 *	
	 *	Empty strings are causing errors. For that reason 'CASE WHEN ... END' is used
	 */
	public function castToNumeric($colName)
	{
		return " (CASE WHEN $colName = '' THEN '0' ELSE $colName END)::float ";
	}

	public function checkByteaOutput() {
		if ($this->isByteaOutputHex === null) {
			$sql = 'SHOW bytea_output';
			$val = $this->db->fetchOne($sql);
			$this->isByteaOutputHex = ($val == 'hex');

			$this->hexEscapeLookup = array(
									'30' => '0',
									'31' => '1',
									'32' => '2',
									'33' => '3',
									'34' => '4', 
									'35' => '5',
									'36' => '6',
									'37' => '7',
									'38' => '8',
									'39' => '9',
									'61' => 'a',
									'62' => 'b',
									'63' => 'c',
									'64' => 'd',
									'65' => 'e',
									'66' => 'f'
								);
		}
	}

	// converts the binary to hexadecimal value if required
	// Postgresql is throwing 'character_not_in_reportoire' (22021) error
	// while inserting binary values in bytea columns.
	// This function converts the binary value to hexadecimal
	public function bin2db($value)
	{
		return bin2hex($value);
	}

	// undoes the conversion done by bin2db
	// For version >= 5.4 use hex2bin
	public function db2bin($value)
	{
		if ($this->isByteaOutputHex) {
			$value = $this->hexToEscape($value);
		}

		return pack("H*", $value);
	}

	protected function hexToEscape($value) {
		$parts = str_split($value, 2);
		array_shift($parts); // ignore the first two characters which are "\x"
		$count = count($parts);
		$ret = '';
		for ($i=0; $i<$count; ++$i)
		{
			$ret .= $this->hexEscapeLookup[$parts[$i]];
		}
		return $ret;
	}

	// PDO is returning bytea values as 'Resource(XX) of type'
	// To avoid that, bytea columns as being casted to text.
	// No need for column alias as postgres does not include
	// the cast type in the column name
	public function binaryColumn($col)
	{
		return $col . '::text';
	}

    // remove sql functions from field name
    // example: `HOUR(log_visit.visit_last_action_time)` gets `HOUR(log_visit` => remove `HOUR(` 
	// works for the "EXTRACT HOUR FROM" functions
	public function removeFunctionFromField($field)
	{
		$pos1 = strpos($field, '(');
		if ($pos1 !== false)
		{
			$pos2 = strpos($field, ')');
			if ($pos2 !== false)
			{
				$sub = substr($field, $pos1+1, $pos2-$pos1-1);
			}
			else
			{
				$sub = substr($field, $pos1+1);
			}
			$parts = explode(' ', $sub);
			$parts = array_filter($parts);
			$sub = array_pop($parts);
			$sub = trim($sub);
		}
		else
		{
			$sub = trim($field);
		}

		return $sub;
	}

	private function getColumnsFromWhere($where)
	{

		// extract column names from the where clause
		$columns = array();
		foreach ($where as $w)
		{
			$w = trim($w);
			$pos = strpos($w, ' ');
			$columns[] = substr($w, 0, $pos);
		}

		return implode(', ', $columns);

	}
}