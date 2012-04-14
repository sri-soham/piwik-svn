<?php
/**
 * Piwik - Open source web analytics
 * 
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 * @version $Id: Controller.php 6174 2012-04-07 02:30:49Z capedfuzz $
 * 
 * @category Piwik_Plugins
 * @package Piwik_DBStats
 */

/**
 *
 * @package Piwik_DBStats
 */
class Piwik_DBStats_Controller extends Piwik_Controller_Admin
{
	function index()
	{
		Piwik::checkUserIsSuperUser();
		$view = Piwik_View::factory('DBStats');
		$view->tablesStatus = Piwik_DBStats_API::getInstance()->getAllTablesStatusPretty();
		$this->setBasicVariablesView($view);
		$view->menu = Piwik_GetAdminMenu();
		echo $view->render();		
	}
}