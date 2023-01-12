"""External statistics from Eloverblik"""
from datetime import datetime, timedelta
from random import randint
from functools import partial
import logging
import pytz
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMetaData
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    get_last_statistics
)
from homeassistant.helpers.event import async_call_later, async_track_time_change
from pyeloverblik.models import TimeSeries

from .const import DOMAIN
#from .__init__ import HassEloverblik

_LOGGER = logging.getLogger(__name__)
RANDOM_MINUTE = randint(0, 59)
RANDOM_SECOND = randint(0, 59)


class EloverblikStatistic:
    """This class handles the total energy of the meter,
    and imports it as long term statistics from Eloverblik."""

    def __init__(self, hass: HomeAssistant, client):
        self._hass = hass
        self._client = client
        self._unsub_daily_callback = async_track_time_change(
            hass,
            self._daily_callback,
            hour=5,
            minute=RANDOM_MINUTE,
            second=RANDOM_SECOND
        )

        hass.async_create_task(self._daily_callback())
        #async_call_later(hass, 10, partial(self._callback_new_day))
        #hass.async_run_hass_job

    async def async_unload(self):
        """Used to unload thee statistics object, removing stastics and callback"""
        self._unsub_daily_callback()
        await get_instance(self._hass).async_clear_statistics([self.statistics_id])

    @property
    def name(self):
        """Return the name for the external statistic."""
        return f"Eloverblik Energy {self._client.get_metering_point()}"

    @property
    def statistics_id(self):
        """Return the statistics id for the external statistic."""
        return f"{DOMAIN}:energy_{self._client.get_metering_point()}"

    async def _daily_callback(self, _ = None):
        """Callback on a new day"""
        _LOGGER.warn("Called new_day_cb callback")

        try:
            last_stat = await self._get_last_stat(self._hass)

            if last_stat is not None and pytz.utc.localize(datetime.now()) - last_stat["start"] < timedelta(days=1):
                # If less than 1 day since last record, don't pull new data.
                # Data is available at the earliest a day after.
                return

            inserted_rows = await self._async_update(last_stat)

            if not inserted_rows:
                _LOGGER.warn("No data inserted from Eloverblik, waiting and retrying")
                async_call_later(self._hass, timedelta(hours=1), partial(self._daily_callback))
            else:
                _LOGGER.warn("Statistic from Eloverblik successfully updated")
        except Exception as ex:
            _LOGGER.warning("retrying in 30 seconds", exc_info=True)
            async_call_later(self._hass, timedelta(seconds=30), partial(self._daily_callback))


    async def _async_update(self, last_stat: StatisticData):
        if last_stat is None:
            # if none import from last january
            from_date = datetime(datetime.today().year-1, 1, 1)
        else:
            from_date = last_stat["start"] + timedelta(hours=1)
        from_date = datetime(datetime.today().year-1, 1, 1)

        data = await self._hass.async_add_executor_job(
            self._client.get_hourly_data,
            from_date,
            datetime.now())

        if data is not None:
            await self._insert_statistics(self._hass, data, last_stat)
            return True
        else:
            return False

    async def _insert_statistics(
        self,
        hass: HomeAssistant,
        data: dict[datetime, TimeSeries],
        last_stat: StatisticData):

        statistics : list[StatisticData] = []

        if last_stat is not None:
            total = last_stat["sum"]
        else:
            total = 0

        # Sort time series to ensure correct insertion
        sorted_time_series = sorted(data.values(), key = lambda timeseries : timeseries.data_date)

        for time_series in sorted_time_series:
            number_of_hours = len(time_series._metering_data)

            # data_date returned is end of the time series
            date = pytz.utc.localize(time_series.data_date) - timedelta(hours=number_of_hours)

            for hour in range(0, number_of_hours):
                start = date + timedelta(hours=hour)

                total += time_series.get_metering_data(hour+1)

                statistics.append(
                    StatisticData(
                        start=start,
                        sum=total
                    ))

        metadata = StatisticMetaData(
            name=self.name,
            source=DOMAIN,
            statistic_id=self.statistics_id,
            unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
            has_mean=False,
            has_sum=True,
        )

        async_add_external_statistics(hass, metadata, statistics)

    async def _get_last_stat(self, hass: HomeAssistant) -> StatisticData:
        last_stats = await get_instance(hass).async_add_executor_job(
            get_last_statistics, hass, 1, self.statistics_id, True, {"sum"}
        )

        if self.statistics_id in last_stats and len(last_stats[self.statistics_id]) > 0:
            return last_stats[self.statistics_id][0]
        else:
            return None
