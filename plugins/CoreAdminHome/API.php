<?php
/**
 * Piwik - Open source web analytics
 * 
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 * @version $Id: API.php 6959 2012-09-10 07:37:01Z matt $
 * 
 * @category Piwik_Plugins
 * @package Piwik_CoreAdminHome
 */

/**
 * @package Piwik_CoreAdminHome
 */
class Piwik_CoreAdminHome_API 
{
	static private $instance = null;
	/**
	 * @return Piwik_CoreAdminHome_API
	 */
	static public function getInstance()
	{
		if (self::$instance == null)
		{
			self::$instance = new self;
		}
		return self::$instance;
	}

	/**
	 * Will run all scheduled tasks due to run at this time.
	 *
	 * @return array
	 */
	public function runScheduledTasks()
	{
		Piwik::checkUserIsSuperUser();
		return Piwik_TaskScheduler::runTasks();
	}
	
	public function getKnownSegmentsToArchive()
	{
		Piwik::checkUserIsSuperUser();
		return Piwik::getKnownSegmentsToArchive();
	}

	/*
	 * stores the list of websites IDs to re-reprocess in archive.php 
	 */
	const OPTION_INVALIDATED_IDSITES = 'InvalidatedOldReports_WebsiteIds';
	
	/**
	 * When tracking data in the past (using Tracking API), this function
	 * can be used to invalidate reports for the idSites and dates where new data
	 * was added. 
	 * DEV: If you call this API, the UI should display the data correctly, but will process
	 *      in real time, which could be very slow after large data imports. 
	 *      After calling this function via REST, you can manually force all data 
	 *      to be reprocessed by visiting the script as the Super User:
	 *      http://example.net/piwik/misc/cron/archive.php?token_auth=$SUPER_USER_TOKEN_AUTH_HERE 
	 * REQUIREMENTS: On large piwik setups, you will need in PHP configuration: max_execution_time = 0
	 * 	We recommend to use an hourly schedule of the script at misc/cron/archive.php 
	 * 	More information: http://piwik.org/setup-auto-archiving/
	 * 
	 * @param string $idSites Comma separated list of idSite that have had data imported for the specified dates
	 * @param string $dates Comma separated list of dates to invalidate for all these websites
	 * @return array
	 */
	public function invalidateArchivedReports($idSites, $dates)
	{
		Piwik::checkUserIsSuperUser();
		$idSites = Piwik_Site::getIdSitesFromIdSitesString($idSites);
		if(empty($idSites)) {
			throw new Exception("Specify a value for &idSites=");
		}
		// Ensure the specified dates are valid
		$toInvalidate = $invalidDates = array();
		$dates = explode(',', $dates);
		$dates = array_unique($dates);
		foreach($dates as $theDate)
		{
			try {
				$date = Piwik_Date::factory($theDate);
			} catch(Exception $e) {
				$invalidDates[] = $theDate;
				continue;
			}
			if($date->toString() == $theDate)
			{
				$toInvalidate[] = $date;
			}
			else
			{
				$invalidDates[] = $theDate;
			}
		}

		// Lookup archive tables
		$tables = Piwik::getTablesInstalled();
		$archiveTables = Piwik::getTablesArchivesInstalled();
		
		// If using the feature "Delete logs older than N days"...
		$logsAreDeletedBeforeThisDate = Piwik_Config::getInstance()->Deletelogs['delete_logs_schedule_lowest_interval'];
		$logsDeleteEnabled = Piwik_Config::getInstance()->Deletelogs['delete_logs_enable'];
		$minimumDateWithLogs = false;
		if($logsDeleteEnabled
			&& $logsAreDeletedBeforeThisDate) 
		{
			$minimumDateWithLogs = Piwik_Date::factory('today')->subDay($logsAreDeletedBeforeThisDate);
		}
		
		$Archive = Piwik_Db_Factory::getDAO('archive');
			
		// Given the list of dates, process which tables they should be deleted from
		$minDate = false;
		$warningDates = $processedDates = array();
		/* @var $date Piwik_Date */
		foreach($toInvalidate as $date)
		{
			// we should only delete reports for dates that are more recent than N days
			if($minimumDateWithLogs
				&& $date->isEarlier($minimumDateWithLogs))
			{
				$warningDates[] = $date->toString();
			}
			else
			{
				$processedDates[] = $date->toString();
			}
				
			$month = $date->toString('Y_m');
			// For a given date, we must invalidate in the monthly archive table
			$datesByMonth[$month][] = $date->toString();
			
			// But also the year stored in January
			$year = $date->toString('Y_01');
			$datesByMonth[$year][] = $date->toString();
			
			// but also weeks overlapping several months stored in the month where the week is starting
			/* @var $week Piwik_Period_Week */
			$week = Piwik_Period::factory('week', $date);
			$week = $week->getDateStart()->toString('Y_m');
			$datesByMonth[$week][] = $date->toString();
			
			// Keep track of the minimum date for each website 
			if($minDate === false
				|| $date->isEarlier($minDate))
			{
				$minDate = $date;
			}
		}
		
		// In each table, invalidate day/week/month/year containing this date
		$sqlIdSites = implode(",", $idSites);
		foreach($archiveTables as $table)
		{
			// Extract Y_m from table name
			$suffix = str_replace(array('archive_numeric_','archive_blob_'), '', Piwik_Common::unprefixTable($table));
			
			if(!isset($datesByMonth[$suffix]))
			{
				continue;
			}
			// Dates which are to be deleted from this table
			$datesToDeleteInTable = $datesByMonth[$suffix];
			
			// One statement to delete all dates from the given table
			$datesToDeleteInTable = array_unique($datesToDeleteInTable);
			$Archive->deleteByDates($table, $sqlIdSites, $datesToDeleteInTable);
		}

		// Update piwik_site.ts_created 
		$Site = Piwik_Db_Factory::getDAO('site');
		$Site->updateTSCreated($sqlIdSites, $minDate->subDay(1)->getDatetime());

		// Force to re-process data for these websites in the next archive.php cron run
		$invalidatedIdSites = Piwik_CoreAdminHome_API::getWebsiteIdsToInvalidate();
		$invalidatedIdSites = array_merge($invalidatedIdSites, $idSites);
		$invalidatedIdSites = array_unique($invalidatedIdSites);
		$invalidatedIdSites = array_values($invalidatedIdSites);
		Piwik_SetOption(self::OPTION_INVALIDATED_IDSITES, serialize($invalidatedIdSites));
		
		Piwik_Site::clearCache();
		
		$output = array();
		// output logs
		if($warningDates)
		{
			$output[] = 'Warning: the following Dates have not been invalidated, because they are earlier than your Log Deletion limit: '.
						implode(", ", $warningDates).
					"\n The last day with logs is " . $minimumDateWithLogs. ". ".
					"\n Please disable 'Delete old Logs' or set it to a higher deletion threshold (eg. 180 days or 365 years).'.";
		} 
		$output[] = "Success. The following dates were invalidated successfully: ".
						implode(", ", $processedDates);
		return $output;
	}
	
	/**
	 * Returns array of idSites to force re-process next time archive.php runs
	 * 
	 * @ignore
	 * @return mixed
	 */
	static public function getWebsiteIdsToInvalidate()
	{
		Piwik::checkUserIsSuperUser();
		$invalidatedIdSites = Piwik_GetOption(self::OPTION_INVALIDATED_IDSITES);
		if($invalidatedIdSites 
			&& ($invalidatedIdSites = unserialize($invalidatedIdSites))
			&& count($invalidatedIdSites))
		{
			return $invalidatedIdSites;
		}
		return array();
	}
}
