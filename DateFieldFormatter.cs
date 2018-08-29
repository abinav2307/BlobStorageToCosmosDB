using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.Tools.POC.BlobToCosmosDB
{
    internal sealed class DateFieldFormatter
    {
        /// <summary>
        /// Returns the week of the year for the input DateTime
        /// </summary>
        /// <param name="time">DateTime from which to return the week of year</param>
        /// <returns></returns>
        private static int GetIso8601WeekOfYear(DateTime time)
        {
            Calendar cal = CultureInfo.InvariantCulture.Calendar;

            DayOfWeek day = cal.GetDayOfWeek(time);
            if (day >= DayOfWeek.Monday && day <= DayOfWeek.Wednesday)
            {
                time = time.AddDays(3);
            }

            // Return the week of our adjusted day
            return cal.GetWeekOfYear(time, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday);
        }

        /// <summary>
        /// Given a date string, this method converts the date into the desired format
        /// 
        /// Expected date formats are: 
        /// <list type="bullet"
        ///     <item>
        ///         <description> yyyy-MM-dd
        ///         </description>
        ///     </item>
        ///     <item>
        ///         <description> yyyyMMdd
        ///         </description>
        ///     </item>
        ///     <item>
        ///         <description> yyyy/MM/dd
        ///         </description>
        ///     </item>
        /// </list>
        ///
        /// Possible output date format strings are:
        /// <list type="bullet"
        ///     <item>
        ///         <description>yyyy-MM-dd (default)</description>
        ///     </item>
        ///     <item>
        ///         <description>yyyy-MM-dd-WW</description>
        ///     </item>
        ///     <item>
        ///         <description>yyyy </description>
        ///     </item>
        ///     <item>
        ///         <description>MM </description>
        ///     </item>
        ///     <item>
        ///         <description>dd </description>
        ///     </item>
        ///     <item>
        ///         <description>WW </description>
        ///     </item>
        ///     <item>
        ///         <description>yyyy-MM </description>
        ///     </item>
        ///     <item>
        ///         <description>yyyy-dd </description>
        ///     </item>
        ///     <item>
        ///         <description>yyyy-WW </description>
        ///     </item>
        ///     <item>
        ///         <description>MM-dd </description>
        ///     </item>
        ///     <item>
        ///         <description>MM-WW </description>
        ///     </item>
        ///     <item>
        ///         <description>dd-WW </description>
        ///     </item>
        ///
        /// </summary>
        /// <param name="DateString">String representation of the date field/column from user's data</param>
        /// <returns></returns>
        public static string GetDateInRequiredFormat(string DateString)
        {
            string datePattern = null;

            bool dateTimeFormatInYYYY_MM_dd = bool.Parse(ConfigurationManager.AppSettings["DateFormatInYYYY-MM-DD"]);
            bool dateTimeFormatInYYYYMMdd = bool.Parse(ConfigurationManager.AppSettings["DateFormatInYYYYMMDD"]);
            bool dateTimeFormatWithSlash = bool.Parse(ConfigurationManager.AppSettings["DateFormatInYYYY/MM/DD"]);

            if(dateTimeFormatInYYYY_MM_dd)
            {
                datePattern = "yyyy-MM-dd";
            }
            else if(dateTimeFormatInYYYYMMdd)
            {
                datePattern = "yyyyMMdd";
            }
            else if (dateTimeFormatWithSlash)
            {
                datePattern = "yyyy/MM/dd";
            }

            DateTime dateTime;
            bool useYYYYMMDD = bool.Parse(ConfigurationManager.AppSettings["Use-YYYYMMDD"]);
            if (useYYYYMMDD)
            {
                if(DateTime.TryParseExact(DateString, datePattern, null, DateTimeStyles.None, out dateTime))
                {
                    return dateTime.ToString();
                }

                ThrowArgumenException(DateString, datePattern);
            }

            bool useYYYYMMDDWW = bool.Parse(ConfigurationManager.AppSettings["Use-YYYYMMDD-WW"]);
            if (useYYYYMMDDWW)
            {
                if (DateTime.TryParseExact(DateString, datePattern, null, DateTimeStyles.None, out dateTime))
                {
                    int WW = GetIso8601WeekOfYear(dateTime);
                    return string.Concat(dateTime.ToString("MM-dd"), "-", WW);
                }

                ThrowArgumenException(DateString, datePattern);
            }

            bool useOnlyYYYY = bool.Parse(ConfigurationManager.AppSettings["Use-OnlyYYYY"]);
            if (useOnlyYYYY)
            {
                if (DateTime.TryParseExact(DateString, datePattern, null, DateTimeStyles.None, out dateTime))
                {
                    int WW = GetIso8601WeekOfYear(dateTime);
                    return dateTime.ToString("yyyy");
                }

                ThrowArgumenException(DateString, datePattern);
            }

            bool useOnlyMM = bool.Parse(ConfigurationManager.AppSettings["Use-OnlyMM"]);
            if (useOnlyMM)
            {
                if (DateTime.TryParseExact(DateString, datePattern, null, DateTimeStyles.None, out dateTime))
                {
                    int WW = GetIso8601WeekOfYear(dateTime);
                    return dateTime.ToString("MM");
                }

                ThrowArgumenException(DateString, datePattern);
            }

            bool useOnlyDD = bool.Parse(ConfigurationManager.AppSettings["Use-OnlyDD"]);
            if (useOnlyDD)
            {
                if (DateTime.TryParseExact(DateString, datePattern, null, DateTimeStyles.None, out dateTime))
                {
                    int WW = GetIso8601WeekOfYear(dateTime);
                    return dateTime.ToString("dd");
                }

                ThrowArgumenException(DateString, datePattern);
            }

            bool useOnlyWW = bool.Parse(ConfigurationManager.AppSettings["Use-OnlyWW"]);
            if (useOnlyWW)
            {
                if (DateTime.TryParseExact(DateString, datePattern, null, DateTimeStyles.None, out dateTime))
                {
                    int WW = GetIso8601WeekOfYear(dateTime);
                    return WW.ToString();
                }

                ThrowArgumenException(DateString, datePattern);
            }

            bool useOnlyYYMM = bool.Parse(ConfigurationManager.AppSettings["Use-OnlyYYMM"]);
            if (useOnlyYYMM)
            {
                if (DateTime.TryParseExact(DateString, datePattern, null, DateTimeStyles.None, out dateTime))
                {
                    int WW = GetIso8601WeekOfYear(dateTime);
                    return dateTime.ToString("yyyy-MM");
                }

                ThrowArgumenException(DateString, datePattern);
            }

            bool useOnlyYYDD = bool.Parse(ConfigurationManager.AppSettings["Use-OnlyYYDD"]);
            if (useOnlyYYDD)
            {
                if (DateTime.TryParseExact(DateString, datePattern, null, DateTimeStyles.None, out dateTime))
                {
                    int WW = GetIso8601WeekOfYear(dateTime);
                    return dateTime.ToString("yyyy-dd");
                }

                ThrowArgumenException(DateString, datePattern);
            }

            bool useOnlyYYWW = bool.Parse(ConfigurationManager.AppSettings["Use-OnlyYYWW"]);
            if (useOnlyYYWW)
            {
                if (DateTime.TryParseExact(DateString, datePattern, null, DateTimeStyles.None, out dateTime))
                {
                    int WW = GetIso8601WeekOfYear(dateTime);
                    return string.Concat(dateTime.ToString("yyyy"), WW.ToString());
                }

                ThrowArgumenException(DateString, datePattern);
            }

            bool useOnlyMMDD = bool.Parse(ConfigurationManager.AppSettings["Use-OnlyMMDD"]);
            if (useOnlyMMDD)
            {
                if (DateTime.TryParseExact(DateString, datePattern, null, DateTimeStyles.None, out dateTime))
                {
                    int WW = GetIso8601WeekOfYear(dateTime);
                    return dateTime.ToString("MM-dd");
                }

                ThrowArgumenException(DateString, datePattern);
            }

            bool useOnlyMMWW = bool.Parse(ConfigurationManager.AppSettings["Use-OnlyMMWW"]);
            if (useOnlyMMWW)
            {
                if (DateTime.TryParseExact(DateString, datePattern, null, DateTimeStyles.None, out dateTime))
                {
                    int WW = GetIso8601WeekOfYear(dateTime);
                    return string.Concat(dateTime.ToString("MM"), WW.ToString());
                }

                ThrowArgumenException(DateString, datePattern);
            }

            bool useOnlyDDWW = bool.Parse(ConfigurationManager.AppSettings["Use-OnlyDDWW"]);
            if (useOnlyDDWW)
            {
                if (DateTime.TryParseExact(DateString, datePattern, null, DateTimeStyles.None, out dateTime))
                {
                    int WW = GetIso8601WeekOfYear(dateTime);
                    return string.Concat(dateTime.ToString("dd"), WW.ToString());
                }

                ThrowArgumenException(DateString, datePattern);
            }

            return null;
        }

        /// <summary>
        /// Throw ArgumentException if date string does not follow the allowable formats
        /// </summary>
        /// <param name="dateString"></param>
        /// <param name="datePattern"></param>
        private static void ThrowArgumenException(string dateString, string datePattern)
        {
            string exceptionDetail = string.Format("Invalid date format: {0}. Date not in expected format: {1}", dateString, datePattern);
            throw new ArgumentException(exceptionDetail);
        }
    }
}
