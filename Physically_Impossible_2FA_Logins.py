import datetime
import duo_client
from file_locations import duo_logger, duo_log_csv, ta_df_csv
import keyring
import gc
import os
import pandas as pd
import time

# Make df more reader friendly in 'Run' window
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('expand_frame_repr', False)

# Start timer
start = time.time()


class DUO:
    def __init__(self, ikey, skey, host, days_to_go_back, hours_to_compare):
        try:
            # Create admin_api instance to pull data
            self.admin_api = duo_client.Admin(ikey=ikey,
                                              skey=skey,
                                              host=host)

            self.days_back = days_to_go_back
            self.hours_to_compare = hours_to_compare
            duo_logger.info(f'Successfully imported API credentials.')

            self.pull_in_data()

        except Exception as e:
            duo_logger.error(f'Failed importing API credentials. Error is {e}.')


    def pull_in_data(self):

        try:
            # Decide how far back the algorith will pull data in UTC time
            now = datetime.datetime.utcnow()
            mintime_ms = int((now - datetime.timedelta(days=self.days_back)).timestamp() * 1000)
            maxtime_ms = int(now.timestamp() * 1000)

            # Create blank dataframe to insert data into
            df = pd.DataFrame()

            # Create an arbitrary range to iterate over. We have a break point so this will never come close to 100
            for x in range(100):
                if x == 0:
                    logs = self.admin_api.get_authentication_log(api_version=2, mintime=mintime_ms, maxtime=maxtime_ms,
                                                                 limit='1000')
                    x = 1
                    # Get the next_offset
                    next_offset = logs['metadata']['next_offset']
                    # Insert data into df
                    df = pd.json_normalize(logs['authlogs'])

                else:
                    logs = self.admin_api.get_authentication_log(api_version=2, mintime=mintime_ms, maxtime=maxtime_ms,
                                                                 limit='1000', next_offset=next_offset)
                    # Get the next_offset
                    next_offset = logs['metadata']['next_offset']

                    # If next_offset is None we have all the data. Time to stop the loop.
                    if next_offset is None:
                        break

                    # Create temp df to insert data to.
                    temp_df = pd.json_normalize(logs['authlogs'])
                    # Append data to main df
                    df = df.append(temp_df, ignore_index=True)
                    # Delete temp df
                    del temp_df
                    # Force garbage collection to wipe it from memory
                    gc.collect()

            # Drop any duplicates that may arise based off timestamp
            df = df.drop_duplicates(subset='isotimestamp', keep='first')

            # Drop empty rows in column
            df.dropna(subset=['access_device.location.state'], inplace=True)

            # Drop specific service accounts
            df.drop(df.loc[df['user.name'] == 'fimsusr'].index, inplace=True)

            # Get unique login names
            names = df['user.name'].unique()

            ta_df = pd.DataFrame()
            for name in names:
                temp_df = df.loc[df['user.name'] == name]
                # print(temp_df)
                states = temp_df['access_device.location.state'].unique()
                # If length of states is greater than 1 than multiples states were logged into.

                # If more than 1 state
                if len(states) > 1:
                    ta_df = ta_df.append(temp_df, ignore_index=True)

            # # Convert column "timestamp" to datetime
            ta_df['timestamp_'] = pd.to_datetime(ta_df.timestamp, unit='s', utc=True)

            # Delete tmp_df, name, names
            del temp_df, name, names

            # Call garbage collection
            gc.collect()

            names = ta_df['user.name'].unique()

            # Iterate over df to compare times
            ta_list = []
            for ta_name in names:
                temp_df = ta_df.loc[ta_df['user.name'] == ta_name].copy(deep=True)

                # Sort by datetime
                temp_df.sort_values(by='timestamp_', ascending=False, inplace=True)

                time_ = temp_df['timestamp_'].tolist()
                state_ = temp_df['access_device.location.state'].tolist()

                # Convert lists to dictionary
                ta_dictionary = {}
                for key in time_:
                    for value in state_:
                        ta_dictionary[key] = value
                        state_.remove(value)
                        break

                counter = 0

                # Iterate over dictionary to find physically impossible logins
                # Times are keys. States are values.
                for key, value in ta_dictionary.items():
                    # Increment counter
                    counter += 1
                    # If counter is odd
                    if counter % 2 != 0:
                        key_1 = key
                        value_1 = value

                        # Skip first odd number (which is 1)
                        if counter in (3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25):
                            hour_comparison = key_2 - key_1

                            # Compare hour differences. If less than specified & different states then flag it
                            if hour_comparison < datetime.timedelta(hours=self.hours_to_compare):
                                # If states are different pull name
                                if value_2 != value_1:
                                    ta_list.append(ta_name)

                    elif counter % 2 == 0:
                        key_2 = key
                        value_2 = value
                        hour_comparison = key_1 - key_2

                        # Compare hour differences. If less than specified & different states then flag it
                        if hour_comparison < datetime.timedelta(hours=self.hours_to_compare):
                            # If states are different pull name
                            if value_1 != value_2:
                                ta_list.append(ta_name)

                del temp_df
                gc.collect()

            duo_logger.info(f'List of folks to review {ta_list}.')

            # Send file to save location. This is the file to review.
            ta_df.to_csv(ta_df_csv, index=False)
            duo_logger.info(f'Code took {((time.time() - start_time) / 60):.3f} minutes to execute so far.')

        except Exception as e:
            duo_logger.error(f'Error is: {e}.')


if __name__ == '__main__':
    # Duo Admin API
    NAMESPACE = "Duo"
    ikey = 'ikey'
    skey = 'skey'
    host = "host"

    DUO(ikey=keyring.get_password(NAMESPACE, ikey),
        skey=keyring.get_password(NAMESPACE, skey),
        host=keyring.get_password(NAMESPACE, host),
        # How many days do you want to go back for data ingestion?
        days_to_go_back=1,
        # How many hours for comparison? If 2 is listed for example, then if 2 different states pop in a 2-hour interval
        # the algorithm will flag it.
        hours_to_compare=2,
        )


