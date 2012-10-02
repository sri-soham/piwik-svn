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

class Piwik_Db_DAO_Pgsql_LogAction extends Piwik_Db_DAO_LogAction
{ 
	const TEMP_TABLE_NAME = 'tmp_log_actions_to_keep';

	private $generic;

	public function __construct($db, $table)
	{
		parent::__construct($db, $table);
	}

	// to overcome the "could not determine datatype of parameter $4 error"
	// for the bound variables like '*\_)\%'
	// sample sql that can generate the error
	// SELECT CONCAT('%', ?, '%') AS name;
	// array('*\_)\%') or any string like array('piwik')
	public function sqlIdactionFromSegment($matchType, $actionType)
	{
		$sql = 'SELECT idaction FROM ' . $this->table . ' WHERE ';
		switch ($matchType)
		{
			case '=@':
				$sql .= "(name LIKE '%' || ? || '%' AND type = $actionType )";
				break;
			case '!@':
				$sql .= "(name NOT LIKE '%' || ? || '%' AND type = $actionType )";
				break;
			default:
                throw new Exception("This match type is not available for action-segments.");
				break;
		}

		return $sql;
	}

	/**
	 *	add record
	 *
	 *	Adds a record to the log_action table and returns the id of the
	 *	the inserted row.
	 *
	 *	@param string $name
	 *	@param string $type
	 *  @returns int
	 */
	public function add($name, $type)
	{
		$sql = 'INSERT INTO ' . $this->table . ' (name, hash, type) '
			 . 'VALUES (?, ?, ?)';
		$this->db->query($sql, array($name, Piwik_Common::getCrc32($name), $type));

		return $this->db->lastInsertId($this->table.'_idaction');
	}

	/**
	 *	delete Unused actions
	 *
	 *	Deletes the data from log_action table based on the temporary table
	 */
	public function deleteUnusedActions()
	{
		$tempTable = Piwik_Common::prefixTable(self::TEMP_TABLE_NAME);
		$sql = 'DELETE FROM ' . $this->table . ' AS la WHERE NOT EXISTS '
			 . '(SELECT * FROM ' . $tempTable. ' AS tmp ON tmp.idaction = la.idaction)';
		$this->db->query($sql);
	}

	public function purgeUnused($maxIds)
	{
		$this->generic = Piwik_Db_Factory::getGeneric($this->db);
		$this->createTempTable();

		// do large insert (inserting everything before maxIds) w/o locking tables...
		$this->insertActionsToKeep($maxIds, $deleteOlderThanMax = true);

		// ... then do small insert w/ locked tables to minimize the amount of time tables are locked.
		$this->generic->beginTransaction();
		$this->lockLogTables($generic);
		$this->insertActionsToKeep($maxIds, $deleteOlderThanMax = false);
		
		// delete before unlocking tables so there's no chance a new log row that references an
		// unused action will be inserted.
		$this->deleteUnusedActions();
		// unlock the log tables
		$this->generic->commit();
		$this->generic = null;
	}

	protected function insertActionsToKeep($maxIds, $olderThan = true)
	{
		$tempTable = Piwik_Common::prefixTable(self::TEMP_TABLE_NAME);
		$idvisitCondition = $olderThan ? ' idvisit <= ? ' : ' idvisit > ? ';
		foreach ($this->getIdActionColumns as $table => $column)
		{
			foreach ($columns as $col)
			{
				$select = "SELECT $col from " . Piwik_Common::prefixTable($table) . " WHERE $idvisitCondition";
				$sql = "INSERT IGNORE INTO $tempTable $select";
				$this->generic->insertIgnore($sql, array($maxIds[$table]));
			}
		}

		// allow code to be executed after data is inserted. for concurrency testing purposes.
		if ($olderThan)
		{
			Piwik_PostEvent("LogDataPurger.actionsToKeepInserted.olderThan");
		}
		else
		{
			Piwik_PostEvent("LogDataPurger.actionsToKeepInserted.newerThan");
		}
	}

	protected function lockLogTables($generic)
	{
		$generic->lockTables(
			$readLocks = Piwik_Common::prefixTables('log_conversion',
													'log_link_visit_action',
													'log_visit',
													'log_conversion_item'
													),
			$writeLocks = Piwik_Common::prefixTable('log_action')
		);
	}

	protected function createTempTable()
	{
		$sql = 'CREATE TEMPORARY TABLE ' . Piwik_Common::prefixTable(self::TEMP_TABLE_NAME) . '( '
			  .'  idaction INT, '
			  .'  PRIMARY KEY(idaction) '
			  .' );';
		$this->db->query($sql);
	}
} 
