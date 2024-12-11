"""
Market calendar module for handling exchange trading schedules.

This module provides the MarketCalendar class for managing market schedules, holidays,
and trading hours for financial exchanges.
"""
import warnings
from abc import ABCMeta, abstractmethod
from datetime import time

import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay

from .class_registry import RegisteryMeta, ProtectedDict

MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY = range(7)

WEEKMASK_ABBR = {
    MONDAY: "Mon",
    TUESDAY: "Tue",
    WEDNESDAY: "Wed",
    THURSDAY: "Thu",
    FRIDAY: "Fri",
    SATURDAY: "Sat",
    SUNDAY: "Sun",
}


class DEFAULT:
    pass


class MarketCalendarMeta(ABCMeta, RegisteryMeta):
    pass


class MarketCalendar(metaclass=MarketCalendarMeta):
    """
    Represents timing information for a market or exchange.

    Handles market schedules, holidays, trading hours and special events for financial exchanges.
    All times are in UTC unless otherwise specified and use Pandas data structures.

    Attributes:
        regular_market_times : dict
            Default market open/close times
        open_close_map : dict
            Maps market events to open/close status
    """

    regular_market_times = {
        "market_open": ((None, time(0)),),
        "market_close": ((None, time(23)),),
    }

    open_close_map = {
        "market_open": True,
        "market_close": False,
        "break_start": False,
        "break_end": True,
        "pre": True,
        "post": False,
    }

    @staticmethod
    def _tdelta(t, day_offset=0):
        try:
            return pd.Timedelta(
                days=day_offset,
                hours=t.hour,
                minutes=t.minute,
                seconds=t.second
            )
        except AttributeError:
            t, day_offset = t
            return pd.Timedelta(
                days=day_offset,
                hours=t.hour,
                minutes=t.minute,
                seconds=t.second
            )

    @staticmethod
    def _off(tple):
        try:
            return tple[2]
        except IndexError:
            return 0

    @classmethod
    def calendar_names(cls):
        """
        Get all registered market calendar names and aliases.

        Returns:
            list[str]
                List of calendar names that can be used in factory method.
        """
        return [
            cal
            for cal in cls._regmeta_class_registry.keys()
            if cal not in ["MarketCalendar", "TradingCalendar"]
        ]

    @classmethod
    def factory(
        cls, name, *args, **kwargs
    ):
        """
        Create a market calendar instance by name.

        Args:
            name : str
                Name of the MarketCalendar to create.

            *args : tuple
                Additional positional arguments passed to the
                    calendar constructor.

            **kwargs : dict
                Additional keyword arguments passed to the
                    calendar constructor.

        Returns:
            MarketCalendar
                Instance of the requested calendar type

        """
        return

    def __init__(self, open_time=None, close_time=None):
        """Initialize a market calendar.
        Args:
            open_time : datetime.time, optional
                Market open time override. Default is None.
            close_time : datetime.time, optional
                Market close time override. Default is None.
        """

        self.regular_market_times = self.regular_market_times.copy()
        self.open_close_map = self.open_close_map.copy()
        self._customized_market_times = []

        if open_time is not None:
            self.change_time("market_open", open_time)

        if close_time is not None:
            self.change_time("market_close", close_time)

        if not hasattr(self, "_market_times"):
            self._prepare_regular_market_times()

    @property
    @abstractmethod
    def name(self):
        """Get the market name.

        Returns:
            str
                Name of the market
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def tz(self):
        """
        Get the market timezone.

        Returns:
            timezone
                Timezone for the market
        """
        raise NotImplementedError()

    @property
    def market_times(self):
        """
        Get the market times.

        Returns:
            list
                List of market times
        """
        return self._market_times

    def _prepare_regular_market_times(self):
        """Prepare regular market times.
        Raises:
            ValueError
                Values in open_close_map need to be True or False
        """
        oc_map = self.open_close_map
        assert all(
            isinstance(x, bool) for x in oc_map.values()
        ), "Values in open_close_map need to be True or False"

        regular = self.regular_market_times
        discontinued = ProtectedDict()
        regular_tds = {}

        for market_time, times in regular.items():
            if market_time.startswith("interruption_"):
                raise ValueError("'interruption_' prefix is reserved")

            if times[-1][1] is None:
                discontinued._set(market_time, times[-1][0])
                times = times[:-1]
                regular._set(market_time, times)

            regular_tds[market_time] = tuple(
                (t[0], self._tdelta(t[1], self._off(t))) for t in times
            )

        if discontinued:
            warnings.warn(
                f"{list(discontinued.keys())} are discontinued, the dictionary"
                f" `.discontinued_market_times` has the dates on which these were "
                f"discontinued. The times as of those dates are incorrect, use "
                f".remove_time(market_time) to ignore a market_time."
            )

        self.discontinued_market_times = discontinued
        self.regular_market_times = regular

        self._regular_market_timedeltas = regular_tds
        self._market_times = sorted(
            regular.keys(),
            key=lambda x: regular_tds[x][-1][1]
        )
        self._oc_market_times = list(
            filter(oc_map.__contains__, self._market_times)
        )

    def _set_time(self, market_time, times, opens):
        """Set a market time.

        Args:
            market_time : str
                The market time identifier to change
            times : datetime.time or tuple
                New time information
            opens : bool or None, optional
                Whether this time opens (True) or closes (False) the market.
                None means ignore for open/close status.
                Default is DEFAULT which uses class default.
        """
        if isinstance(times, (tuple, list)):
            if not isinstance(times[0], (tuple, list)):
                if times[0] is None:
                    times = (times,)
                else:
                    times = ((None, times[0], times[1]),)
        else:
            times = ((None, times),)

        ln = len(times)
        for i, t in enumerate(times):
            try:
                assert (
                    t[0] is None
                    or isinstance(t[0], str)
                    or isinstance(t[0], pd.Timestamp)
                )
                assert isinstance(t[1], time) or (
                    ln > 1 and i == ln - 1 and t[1] is None
                )
                assert isinstance(self._off(t), int)
            except AssertionError:
                raise AssertionError(
                    "The passed time information is not in the right format, "
                    "please consult the docs for how to set market times"
                )

        if opens is DEFAULT:
            opens = self.__class__.open_close_map.get(market_time, None)

        if opens in (True, False):
            self.open_close_map._set(market_time, opens)

        elif opens is None:
            try:
                self.open_close_map._del(market_time)
            except KeyError:
                pass
        else:
            raise ValueError(
                "when you pass `opens`, it needs to be True, False, or None"
            )

        self.regular_market_times._set(market_time, times)

        if not self.is_custom(market_time):
            self._customized_market_times.append(market_time)

        self._prepare_regular_market_times()

    def change_time(self, market_time, times, opens=DEFAULT):
        """
        Change a market time in regular_market_times.

        Args:
            market_time : str
                The market time identifier to change
            times : datetime.time or tuple
                New time information
            opens : bool or None, optional
                Whether this time opens (True) or closes (False) the market.
                None means ignore for open/close status.
                Default is DEFAULT which uses class default.

        Returns:
            None
        """
        assert market_time in self.regular_market_times, (
            f"{market_time} is not in regular_market_times:"
            f"\n{self._market_times}."
        )
        return self._set_time(market_time, times, opens)

    def add_time(self, market_time, times, opens=DEFAULT):
        """
        Add a new market time.

        Args:
            market_time : str
                The market time identifier to add
            times : datetime.time or tuple
                Time information
            opens : bool or None, optional
                Whether this time opens (True) or closes (False) the market.
                None means ignore for open/close status.
                Default is DEFAULT which uses class default.

        Returns:
            None
        """
        assert market_time not in self.regular_market_times, (
            f"{market_time} is already in regular_market_times:"
            f"\n{self._market_times}"
        )

        return self._set_time(market_time, times, opens)

    def remove_time(self, market_time):
        """
        Remove a market time.

        Args:
            market_time : str
                The market time identifier to remove

        Returns:
            None
                Modifies calendar state in place

        Examples:
            ```python
            calendar.remove_time("market_open")
            calendar.remove_time("break_start")
            ```
        """

        self.regular_market_times._del(market_time)
        try:
            self.open_close_map._del(market_time)
        except KeyError:
            pass

        self._prepare_regular_market_times()
        if self.is_custom(market_time):
            self._customized_market_times.remove(market_time)

    def is_custom(self, market_time):
        return market_time in self._customized_market_times

    @property
    def has_custom(self):
        return len(self._customized_market_times) > 0

    def is_discontinued(self, market_time):
        return market_time in self.discontinued_market_times

    @property
    def has_discontinued(self):
        return len(self.discontinued_market_times) > 0

    def get_time(self, market_time, all_times=False):
        try:
            times = self.regular_market_times[market_time]
        except KeyError as e:
            if "break_start" in market_time or "break_end" in market_time:
                return None
            elif market_time in ["market_open", "market_close"]:
                raise NotImplementedError("You need to set market_times")
            else:
                raise e

        if all_times:
            return times
        return times[-1][1].replace(tzinfo=self.tz)

    def get_time_on(self, market_time, date):
        times = self.get_time(market_time, all_times=True)
        if times is None:
            return None

        date = pd.Timestamp(date)
        for d, t in times[::-1]:
            if d is None or pd.Timestamp(d) < date:
                return t.replace(tzinfo=self.tz)

    def open_time_on(self, date):
        return self.get_time_on("market_open", date)

    def close_time_on(self, date):
        return self.get_time_on("market_close", date)

    def break_start_on(self, date):
        return self.get_time_on("break_start", date)

    def break_end_on(self, date):
        return self.get_time_on("break_end", date)

    @property
    def open_time(self):
        """
        Get default market open time.

        Returns:
            datetime.time
                Default open time
        """
        return self.get_time("market_open")

    @property
    def close_time(self):
        """
        Get default market close time.

        Returns:
            datetime.time
                Default close time
        """
        return self.get_time("market_close")

    @property
    def break_start(self):
        """
        Get market break start time.

        Returns:
            datetime.time or None
                Break start time if exists, else None
        """
        return self.get_time("break_start")

    @property
    def break_end(self):
        """
        Get market break end time.

        Returns:
            datetime.time or None
                Break end time if exists, else None
        """
        return self.get_time("break_end")

    @property
    def regular_holidays(self):
        """
        Get regular market holidays.

        Returns:
            pd.AbstractHolidayCalendar or None
                Calendar of regular holidays
        """
        return None

    @property
    def adhoc_holidays(self):
        """
        Get ad-hoc market holidays.

        Returns:
            list
                List of ad-hoc holiday dates
        """
        return []

    @property
    def weekmask(self):
        return "Mon Tue Wed Thu Fri"

    @property
    def special_opens(self):
        """
        Get special market open times.

        Returns:
            list
                List of (time, calendar) tuples for special opens
        """
        return []

    @property
    def special_opens_adhoc(self):
        """
        Get ad-hoc special market opens.

        Returns:
            list
                List of (time, dates) tuples for special opens
        """
        return []

    @property
    def special_closes(self):
        """
        Get special market close times.

        Returns:
            list
                List of (time, calendar) tuples for special closes
        """
        return []

    @property
    def special_closes_adhoc(self):
        """
        Get ad-hoc special market closes.

        Returns:
            list
                List of (time, dates) tuples for special closes
        """
        return []

    def get_special_times(self, market_time):
        return getattr(self, "special_" + market_time, [])

    def get_special_times_adhoc(self, market_time):
        return getattr(self, "special_" + market_time + "_adhoc", [])

    def get_offset(self, market_time):
        return self._off(self.get_time(market_time, all_times=True)[-1])

    @property
    def open_offset(self):
        """
        Get market open offset.

        Returns:
            int
                Open time offset
        """
        return self.get_offset("market_open")

    @property
    def close_offset(self):
        """
        Get market close offset.

        Returns:
            int
                Close time offset
        """
        return self.get_offset("market_close")

    @property
    def interruptions(self):
        """
        Get market interruption schedule.

        Returns:
            list
                List of (date, start_time, end_time) tuples for interruptions
        """
        return []

    def _convert(self, col: pd.Series):
        col = col.dropna()
        try:
            times = col.str[0]
        except AttributeError:
            return (
                (
                    pd.to_timedelta(
                        col.astype("string").fillna(""),
                        errors="coerce"
                    )
                    + col.index
                )
                .dt.tz_localize(self.tz)
                .dt.tz_convert("UTC")
            )

        return (
            (
                pd.to_timedelta(
                    times.fillna(col).astype("string").fillna(""),
                    errors="coerce"
                )
                + pd.to_timedelta(col.str[1].fillna(0), unit="D")
                + col.index
            )
            .dt.tz_localize(self.tz)
            .dt.tz_convert("UTC")
        )

    @staticmethod
    def _col_name(n: int):
        return (
            f"interruption_start_{n // 2 + 1}"
            if n % 2 == 1
            else f"interruption_end_{n // 2}"
        )

    @property
    def interruptions_df(self):
        """
        Get market interruptions as DataFrame.

        Returns:
            pd.DataFrame
                DataFrame containing interruption schedule
        """
        if not self.interruptions:
            return pd.DataFrame(index=pd.DatetimeIndex([]))
        intr = pd.DataFrame(self.interruptions)
        intr.index = pd.to_datetime(intr.pop(0))

        intr.columns = map(self._col_name, intr.columns)
        intr.index.name = None

        return intr.apply(self._convert).sort_index()

    def holidays(self):
        """
        Get complete holiday calendar.

        Returns:
            CustomBusinessDay
                Calendar containing all holidays
        """
        try:
            return self._holidays
        except AttributeError:
            self._holidays = CustomBusinessDay(
                holidays=self.adhoc_holidays,
                calendar=self.regular_holidays,
                weekmask=self.weekmask,
            )
        return self._holidays

    def valid_days(self, start_date, end_date, tz="UTC"):
        """
        Get valid business days between dates.

        Args:
            start_date : str or datetime
                Start date
            end_date : str or datetime
                End date
            tz : str or timezone
                Timezone for output

        Returns:
            pd.DatetimeIndex
                Index of valid business days
        """
        return pd.date_range(
            start_date, end_date, freq=self.holidays(), normalize=True, tz=tz
        )

    def _get_market_times(self, start, end):
        mts = self._market_times
        return mts[mts.index(start):mts.index(end) + 1]

    def days_at_time(self, days, market_time, day_offset=0):
        """
        Create index of days at specified time.

        Args:
            days : pd.DatetimeIndex
                Base dates
            market_time : str or datetime.time
                Time to apply
            day_offset : int
                Days to offset

        Returns:
            pd.Series
                Dates with time applied
        """
        days = pd.DatetimeIndex(days).tz_localize(None).to_series()

        if isinstance(market_time, str):
            timedeltas = self._regular_market_timedeltas[market_time]
            datetimes = days + timedeltas[0][1]
            for cut_off, timedelta in timedeltas[1:]:
                datetimes = datetimes.where(
                    days < pd.Timestamp(cut_off), days + timedelta
                )

        else:
            datetimes = days + self._tdelta(market_time, day_offset)

        return datetimes.dt.tz_localize(self.tz).dt.tz_convert("UTC")

    def _tryholidays(self, cal, s, e):
        try:
            return cal.holidays(s, e)
        except ValueError:
            return pd.DatetimeIndex([])

    def _special_dates(self, calendars, ad_hoc_dates, start, end):
        indexes = []
        for time_, calendar in calendars:
            if isinstance(calendar, int):
                day_of_week = CustomBusinessDay(weekmask=WEEKMASK_ABBR[calendar])
                indexes.append(
                    self.days_at_time(
                        pd.date_range(start, end, freq=day_of_week), time_
                    )
                )
            else:
                indexes.append(
                    self.days_at_time(
                        self._tryholidays(calendar, start, end),
                        time_
                    )
                )

        indexes += [
            self.days_at_time(dates, time_)
            for time_, dates in ad_hoc_dates
        ]

        if indexes:
            dates = pd.concat(indexes).sort_index().drop_duplicates()
            return dates.loc[start:end.replace(hour=23, minute=59, second=59)]

        return pd.Series([], dtype="datetime64[ns, UTC]", index=pd.DatetimeIndex([]))

    def special_dates(self, market_time, start_date, end_date, filter_holidays=True):
        """
        Get special dates for a market time.

        Args:
            market_time : str
                Market time identifier
            start_date : str or datetime
                Start date
            end_date : str or datetime
                End date
            filter_holidays : bool
                Whether to filter by valid days

        Returns:
            pd.DatetimeIndex
                Special dates for market time
        """
        start_date, end_date = self.clean_dates(start_date, end_date)
        calendars = self.get_special_times(market_time)
        ad_hoc = self.get_special_times_adhoc(market_time)
        special = self._special_dates(calendars, ad_hoc, start_date, end_date)

        if filter_holidays:
            valid = self.valid_days(start_date, end_date, tz=None)
            special = special[special.index.isin(valid)]
        return special

    def schedule(
        self,
        start_date,
        end_date,
        tz="UTC",
        start="market_open",
        end="market_close",
        force_special_times=True,
        market_times=None,
        interruptions=False,
    ):
        """
        Generate market schedule.

        Args:
            start_date : str or datetime
                Start date
            end_date : str or datetime
                End date
            tz : str
                Timezone for output
            start : str
                First market time
            end : str
                Last market time
            force_special_times : bool or None
                How to handle special times
            market_times : list or None
                List of market times to include
            interruptions : bool
                Whether to include interruptions

        Returns:
            pd.DataFrame
                Market schedule
        """
        start_date, end_date = self.clean_dates(start_date, end_date)
        if not (start_date <= end_date):
            raise ValueError("start_date must be before or equal to end_date.")

        _all_days = self.valid_days(start_date, end_date)
        if market_times is None:
            market_times = self._get_market_times(start, end)
        elif market_times == "all":
            market_times = self._market_times

        if not _all_days.size:
            return pd.DataFrame(
                columns=market_times, index=pd.DatetimeIndex([], freq="C")
            )
        _adj_others = force_special_times is True
        _adj_col = force_special_times is not None
        _open_adj = _close_adj = []
        schedule = pd.DataFrame()
        for market_time in market_times:
            temp = self.days_at_time(_all_days, market_time).copy()
            if _adj_col:
                special = self.special_dates(
                    market_time, start_date, end_date, filter_holidays=False
                )
                specialix = special.index[special.index.isin(temp.index)]
                temp.loc[specialix] = special
                if _adj_others:
                    if market_time == "market_open":
                        _open_adj = specialix
                    elif market_time == "market_close":
                        _close_adj = specialix
            schedule[market_time] = temp

        if _adj_others:
            adjusted = schedule.loc[_open_adj].apply(
                lambda x: x.where(x.ge(x["market_open"]), x["market_open"]),
                axis=1
            )
            schedule.loc[_open_adj] = adjusted
            adjusted = schedule.loc[_close_adj].apply(
                lambda x: x.where(x.le(x["market_close"]), x["market_close"]),
                axis=1
            )
            schedule.loc[_close_adj] = adjusted

        if interruptions:
            interrs = self.interruptions_df
            schedule[interrs.columns] = interrs
            schedule = schedule.dropna(how="all", axis=1)

        if tz != "UTC":
            schedule = schedule.apply(lambda s: s.dt.tz_convert(tz))

        return schedule

    def open_at_time(self, schedule, timestamp, include_close=False, only_rth=False):
        """
        Check if market is open at timestamp.

        Args:
            schedule : pd.DataFrame
                Market schedule
            timestamp : str or datetime
                Time to check
            include_close : bool
                Whether to include closing time
            only_rth : bool
                Whether to only check regular trading hours

        Returns:
            bool
                Whether market is open
        """
        timestamp = pd.Timestamp(timestamp)
        try:
            timestamp = timestamp.tz_localize("UTC")
        except TypeError:
            pass

        cols = schedule.columns
        interrs = cols.str.startswith("interruption_")
        if not (cols.isin(self._oc_market_times) | interrs).all():
            raise ValueError(
                "You seem to be using a schedule that isn't based on the market_times, "
                "or includes market_times that are not represented in the open_close_map."
            )

        if only_rth:
            lowest, highest = "market_open", "market_close"
        else:
            cols = cols[~interrs]
            ix = cols.map(self._oc_market_times.index)
            lowest, highest = cols[ix == ix.min()][0], cols[ix == ix.max()][0]

        if timestamp < schedule[lowest].iat[0] or timestamp > schedule[highest].iat[-1]:
            raise ValueError("The provided timestamp is not covered by the schedule")

        day = schedule[schedule[lowest].le(timestamp)].iloc[-1].dropna().sort_values()
        day = day.loc[lowest:highest]
        day = day.index.to_series(index=day)

        if interrs.any():
            starts = day.str.startswith("interruption_start_")
            ends = day.str.startswith("interruption_end_")
            day.loc[starts] = False
            day.loc[ends] = True

        day.loc[day.eq("market_close") & day.shift(-1).eq("post")] = "market_open"
        day = day.map(
            lambda x: (
                self.open_close_map.get(x) if x in self.open_close_map.keys() else x
            )
        )

        if include_close:
            below = day.index < timestamp
        else:
            below = day.index <= timestamp
        return bool(day[below].iat[-1])

    @staticmethod
    def _get_current_time():
        return pd.Timestamp.now(tz="UTC")

    def is_open_now(self, schedule, include_close=False, only_rth=False):
        """
        Check if market is currently open.

        Args:
            schedule : pd.DataFrame
                Market schedule
            include_close : bool
                Whether to include closing time
            only_rth : bool
                Whether to only check regular trading hours

        Returns:
            bool
                Whether market is open
        """
        current_time = MarketCalendar._get_current_time()
        return self.open_at_time(
            schedule, current_time, include_close=include_close, only_rth=only_rth
        )

    def clean_dates(self, start_date, end_date):
        """
        Clean date inputs.

        Args:
            start_date : str or datetime
                Start date
            end_date : str or datetime
                End date

        Returns:
            tuple
                Cleaned start and end dates
        """
        start_date = pd.Timestamp(start_date).tz_localize(None).normalize()
        end_date = pd.Timestamp(end_date).tz_localize(None).normalize()
        return start_date, end_date

    def is_different(self, col, diff=None):
        if diff is None:
            diff = pd.Series.ne
        normal = self.days_at_time(col.index, col.name)
        return diff(col.dt.tz_convert("UTC"), normal)

    def early_closes(self, schedule):
        """
        Get early close dates.

        Args:
            schedule : pd.DataFrame
                Market schedule

        Returns:
            pd.DataFrame
                Schedule rows with early closes
        """
        return schedule[self.is_different(schedule["market_close"], pd.Series.lt)]

    def late_opens(self, schedule):
        """
        Get late open dates.

        Args:
            schedule : pd.DataFrame
                Market schedule

        Returns:
            pd.DataFrame
                Schedule rows with late opens
        """
        return schedule[self.is_different(schedule["market_open"], pd.Series.gt)]

    def __getitem__(self, item):
        if isinstance(item, (tuple, list)):
            if item[1] == "all":
                return self.get_time(item[0], all_times=True)
            else:
                return self.get_time_on(item[0], item[1])
        else:
            return self.get_time(item)

    def __setitem__(self, key, value):
        return self.add_time(key, value)

    def __delitem__(self, key):
        return self.remove_time(key)


__all__ = [
    "MarketCalendar",
    "get_calendar",
    "get_calendar_names",
    "merge_schedules",
    "date_range",
    "convert_freq",
]
