<?php
/**
 * Piwik - Open source web analytics
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 * @version $Id: $
 *
 * @category Piwik_Plugins
 * @package Piwik_PrivacyManager
 */

/**
 * Purges the log_visit, log_conversion and related tables of old visit data.
 */
class Piwik_PrivacyManager_LogDataPurger
{
	/**
	 * The max set of rows each table scan select should query at one time.
	 */
	public static $selectSegmentSize = 100000;
	
	/**
	 * The number of days after which log entries are considered old.
	 */
	private $deleteLogsOlderThan;
	
	/**
	 * The number of rows to delete per DELETE query.
	 */
	private $maxRowsToDeletePerQuery;
	
	/**
	 * Constructor.
	 * 
	 * @param int $deleteLogsOlderThan The number of days after which log entires are considered old.
	 *                                 Visits and related data whose age is greater than this number
	 *                                 will be purged.
	 * @param int $maxRowsToDeletePerQuery The maximum number of rows to delete in one query. Used to
	 *                                     make sure log tables aren't locked for too long.
	 */
	public function __construct( $deleteLogsOlderThan, $maxRowsToDeletePerQuery )
	{
		$this->deleteLogsOlderThan = $deleteLogsOlderThan;
		$this->maxRowsToDeletePerQuery = $maxRowsToDeletePerQuery;
	}
	
	/**
	 * Purges old data from the following tables:
	 * - log_visit
	 * - log_link_visit_action
	 * - log_conversion
	 * - log_conversion_item
	 * - log_action
	 */
	public function purgeData()
	{
		$maxIdVisit = $this->getDeleteIdVisitOffset();
		
		// break if no ID was found (nothing to delete for given period)
		if (empty($maxIdVisit))
		{
			return;
		}
		
		$logTables = self::getDeleteTableLogTables();
		$Generic = Piwik_Db_Factory::getGeneric();
		
		// delete data from log tables
		$where = array('idvisit <= ?');
		foreach ($logTables as $logTable)
		{
			// deleting from log_action must be handled differently, so we do it later
			if ($logTable != Piwik_Common::prefixTable('log_action'))
			{
				$Generic->deleteAll($logTable, $where, $this->maxRowsToDeletePerQuery, array($maxIdVisit));
			}
		}
		
		// delete unused actions from the log_action table (but only if we can lock tables)
		if (Piwik::isLockPrivilegeGranted())
		{
			$this->purgeUnusedLogActions();
		}
		else
		{
			$logMessage = get_class($this).": LOCK TABLES privilege not granted; skipping unused actions purge";
			Piwik::log($logMessage);
		}
		
		// optimize table overhead after deletion
		Piwik_OptimizeTables($logTables);
	}
	
	/**
	 * Returns an array describing what data would be purged if purging were invoked.
	 * 
	 * This function returns an array that maps table names with the number of rows
	 * that will be deleted.
	 * 
	 * @return array
	 */
	public function getPurgeEstimate()
	{
		$result = array();
		
		// deal w/ log tables that will be purged
		$maxIdVisit = $this->getDeleteIdVisitOffset();
		if (!empty($maxIdVisit))
		{
			foreach ($this->getDeleteTableLogTables() as $table)
			{
				// getting an estimate for log_action is not supported since it can take too long
				if ($table != Piwik_Common::prefixTable('log_action'))
				{
					$TableDAO = Piwik_Db_Factory::getDAO($table);
					$rowCount = $TableDAO->getCountByIdvisit($maxIdVisit);
					if ($rowCount > 0)
					{
						$result[$table] = $rowCount;
					}
				}
			}
		}
		
		return $result;
	}
	
	/**
	 * Safely delete all unused log_action rows.
	 */
	private function purgeUnusedLogActions()
	{
		// get current max visit ID in log tables w/ idaction references.
		$maxIds = $this->getMaxVisitIdsInLogTables();
		$LogAction = Piwik_Db_Factory::getDAO('log_action');
		$LogAction->purgeUnused($maxIds);
	}
	
	/**
	 * get highest idVisit to delete rows from
	 * @return string
	 */
	private function getDeleteIdVisitOffset()
	{
		$LogVisit = Piwik_Db_Factory::getDAO('log_visit');
		$logVisit = Piwik_Common::prefixTable("log_visit");
		
		// get max idvisit
		$maxIdVisit = $LogVisit->getMaxIdvisit();
		if (empty($maxIdVisit))
		{
			return false;
		}
		
		// select highest idvisit to delete from
		$dateStart = Piwik_Date::factory("today")->subDay($this->deleteLogsOlderThan);
		return $LogVisit->getDeleteIdVisitOffset(
				  $dateStart->toString('Y-m-d H:i:s'),
				  $maxIdVisit,
				  -self::$selectSegmentSize
				);
	}

	private function getMaxVisitIdsInLogTables()
	{
		$tables = array('log_conversion', 'log_link_visit_action', 'log_visit', 'log_conversion_item');
		
		$result = array();
		foreach ($tables as $table)
		{
			$dao = Piwik_Db_Factory::getDAO($table);
			$result[$table] = $dao->getMaxIdvisit();
		}
		
		return $result;
	}
	
	// let's hardcode, since these are not dynamically created tables
	public static function getDeleteTableLogTables()
	{
		$result = Piwik_Common::prefixTables('log_conversion',
											 'log_link_visit_action',
											 'log_visit',
											 'log_conversion_item');
		if (Piwik::isLockPrivilegeGranted())
		{
			$result[] = Piwik_Common::prefixTable('log_action');
		}
		return $result;
	}

	/**
	 * Utility function. Creates a new instance of LogDataPurger with the supplied array
	 * of settings.
	 *
	 * $settings must contain values for the following keys:
	 * - 'delete_logs_older_than': The number of days after which log entries are considered
	 *                             old.
	 * - 'delete_logs_max_rows_per_query': Max number of rows to DELETE in one query.
	 *
	 * @param array $settings Array of settings
	 * @param bool $useRealTable
	 * @return Piwik_PrivacyManager_LogDataPurger
	 */
	public static function make( $settings, $useRealTable = false )
	{
		return new Piwik_PrivacyManager_LogDataPurger(
			$settings['delete_logs_older_than'],
			$settings['delete_logs_max_rows_per_query']
		);
	}
}

