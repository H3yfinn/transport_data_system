import datetime
import pandas as pd
# set the option to suppress the warning: PerformanceWarning: indexing past lexsort depth may impact performance.
pd.options.mode.chained_assignment = None
import numpy as np
import os
import re
import sys
from PIL import Image
import matplotlib.pyplot as plt
import logging
logger = logging.getLogger(__name__)

#ignore warning RuntimeWarning: invalid value encountered in cast
#   values = values.astype(str)
import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)


def interpolate_missing_values(final_combined_data_concordance,INDEX_COLS,paths_dict,automatic_interpolation_method = 'linear', automatic_interpolation = True, FILE_DATE_ID='',percent_of_values_needed_to_interpolate=0.7, INTERPOLATION_LIMIT=3,load_progress=True):
    # #TEMPORARY
    # #remove data from dates after 2019 (have to format it as it is currently a string)
    # final_combined_data_concordance['date'] = pd.to_datetime(final_combined_data_concordance['date'])
    # final_combined_data_concordance = final_combined_data_concordance.loc[final_combined_data_concordance.date < '2020-01-01']

    #Remove year from the current cols without removing it from original list, and set it as a new list
    INDEX_COLS_no_year = INDEX_COLS.copy()
    INDEX_COLS_no_year.remove('date')

    #set interpoaltuion methods. If the method is one that requires an order then make sure to add the order as a number right after the method name
    interpolation_methods = ['linear', 'spline2', 'spline4'] 
    #start a timer
    start_time = datetime.datetime.now()

    #chekc for dsuplicates in final_combined_data_concordance
    #if there are then we should return them asnd ask the user to remove them
    copy_x = final_combined_data_concordance.copy()
    #make the list cols into strings so we can check for duplicates
    copy_x['potential_datapoints'] = copy_x['potential_datapoints'].astype(str)
    duplicates = copy_x[copy_x.duplicated()]
    if duplicates.shape[0] > 0:
        logging.warning('Duplicates found in final_combined_data_concordance. Please remove them and try again. Returning them now.')
        return duplicates, final_combined_data_concordance

    #make Vlaue column a float(.astype(float)) so we can interpolate it
    final_combined_data_concordance.value = final_combined_data_concordance.value.astype(float)
    
    #split the data into measures to make the dataset smaller and so faster to work on
    skipped_rows = []
    
    measures = final_combined_data_concordance.measure.unique().tolist()
    measures.sort()
    
    for measure in measures:
        break_loop = False
        final_combined_data_concordance_measure = final_combined_data_concordance.loc[final_combined_data_concordance.measure == measure]
        #set indexes
        final_combined_data_concordance_measure = final_combined_data_concordance_measure.set_index(INDEX_COLS_no_year)
        #get the unique index rows for the measure
        unique_index_row_no_years = final_combined_data_concordance_measure.index.unique()
        #get the number of iterations for the measure
        number_of_iterations = len(unique_index_row_no_years)
        #tell the user how many iterations there are for the measure
        logging.info('There are %d iterations for the measure %s', number_of_iterations, measure)
        logging.info('Time taken so far: %s', datetime.datetime.now() - start_time)

        ##############################################################

        #interpolate missing values by iterating through the unique index rows
        for index_row_no_year in unique_index_row_no_years:
            #get the data for the current index row
            current_data = final_combined_data_concordance_measure.loc[index_row_no_year]

            #prepare intepolation using a spline and a linear method (could ask chat gpt how to choose the method since there is probably some mathemetical basis for it)
            #so we will create a value column for each interpolation method and fill it with the values in the current data. Then run each interpoaltion on that column and fill in the missing values. Then we will plot the data and ask the user to choose which method to use
            #create a new dataframe to hold the data for the current index row                
            next_iteration, skipped_rows, current_data, final_combined_data_concordance_measure = check_if_enough_values_to_interpolate(current_data,final_combined_data_concordance_measure, index_row_no_year,skipped_rows,INDEX_COLS_no_year,percent_of_values_needed_to_interpolate)

            if next_iteration:
                continue
            current_data = current_data.reset_index().sort_values(by='date')
            ########################################################################################################################################################

            if not automatic_interpolation:
                #MANUAL INTERPOLATION
                #set up plot axes and such
                
                fig, ax = setup_interpolation_timeseries(index_row_no_year)

                interpolation_methods_current = interpolation_methods.copy()

                interpolation_methods_current, current_data, ax, fig = plot_and_test_interpolation_methods(interpolation_methods,interpolation_methods_current, current_data, ax, fig, index_row_no_year,INTERPOLATION_LIMIT)

                #plot original line as well but using a different marker
                ax.plot(current_data['date'], current_data['value'], label='original', marker='o')
                im = show_timeseries(paths_dict,fig)                    

                final_combined_data_concordance_measure, interpolation_method, dataset_selection_method, current_data, user_input_correct, break_loop, im = ask_user_to_choose_interpolation_method(final_combined_data_concordance_measure,im, interpolation_methods_current,current_data, index_row_no_year)

                if break_loop:
                    if im is not None:
                        im.close()
                    else:
                        plt.close('all')
                    break#this occurs if the user inputs the same incorrect value twice, which will probvably occur if they want to quit current process.
            ########################################################################################################################################################
            else:
                #AUTOMATIC INTERPOLATION
                #Here, if the order is too high for the spline method then it will fail so we will try the spline method with a lower order and if that fails then we will try the linear method

                current_data, dataset_selection_method = do_automatic_interpolation(current_data, INTERPOLATION_LIMIT,index_row_no_year,automatic_interpolation_method)
            ########################################################################################################################################################
            ##FINALIZE INTERPOLATION
            #where value is NaN set value to the interpolation value and set dataset_selection_method to 'interpolated'
            
            final_combined_data_concordance_measure = finalise_interpolation(current_data, dataset_selection_method,final_combined_data_concordance_measure, index_row_no_year,INDEX_COLS_no_year)
        
        #make the changes for this measure to the original dataframe and then save that dataframe as csv file to checkpoitn our progress
        final_combined_data_concordance_measure = final_combined_data_concordance_measure.reset_index().set_index(INDEX_COLS)
        final_combined_data_concordance.set_index(INDEX_COLS, inplace=True)

        #repalce the original data with the new data where the index rows amtch
        final_combined_data_concordance.loc[final_combined_data_concordance_measure.index] = final_combined_data_concordance_measure.loc[final_combined_data_concordance_measure.index]

        final_combined_data_concordance.reset_index(inplace=True)

        #now save progress
        filename ='intermediate_data/interpolation/{}_progress.csv'.format(FILE_DATE_ID)
        final_combined_data_concordance.to_csv(filename)
        if break_loop:
            break#this occurs if the user inputs the same incorrect value twice, which will probvably occur if they want to quit current process.

    logging.info('Finished all interpolation')
    #print time it took to run the program
    logging.info('Time taken to run program: {}'.format(datetime.datetime.now() - start_time))
    
    #separate the dataset and source columns
    separate_dataset_and_source(final_combined_data_concordance)

    #and make new_final_combined_data by removing any NA values
    new_final_combined_data = final_combined_data_concordance.dropna(subset=['value'])

    return new_final_combined_data,final_combined_data_concordance

def plot_and_test_interpolation_methods(interpolation_methods,interpolation_methods_current, current_data, ax, fig, index_row_no_year,INTERPOLATION_LIMIT):
    #create a new column for each interpolation method
    for interpolation_method in interpolation_methods:
        #if the method is one that requires an order then it will have  a number as well so check if the value has a number in it
        if re.search(r'\d', interpolation_method):
            #get the order of the polynomial
            order = int(re.search(r'\d', interpolation_method).group())
            #set the interpolation method to the string when you remove the number
            interpolation_method_string = re.sub(r'\d', '', interpolation_method)
            #try the interpolation but it couyld fail ebcause the order is too high
            try:
                #interpolate the values for each interpolation method
                current_data[interpolation_method] = current_data['value'].interpolate(method=interpolation_method_string, order=order, limit_direction='both', limit=INTERPOLATION_LIMIT)
                #set up line in plot
                ax.plot(current_data['date'], current_data[interpolation_method], label=interpolation_method)
                logging.info('Polynomial interpolation succeeded for {} with method {} and order {}'.format(index_row_no_year, interpolation_method_string, order))
            except:
                #if the interpolation fails then notify the user and skip this method
                
                logging.debug('Polynomial interpolation failed for {} with method {} and order {}'.format(index_row_no_year, interpolation_method_string, order))
                logging.debug('Polynomial interpolation failure error printout: ', exc_info=True)

                #remove teh method from the list of methods for this #it seems like something went wrong!!!  loop
                interpolation_methods_current.remove(interpolation_method)
                continue
        else:
            current_data[interpolation_method] = current_data['value']
            #interpolate the values for each interpolation method
            current_data[interpolation_method] = current_data['value'].interpolate(method=interpolation_method, limit_direction='both', limit=INTERPOLATION_LIMIT)
            #set up line in plot
            ax.plot(current_data['date'], current_data[interpolation_method], label=interpolation_method)

    return interpolation_methods_current, current_data, ax, fig

def show_timeseries(paths_dict,fig,use_plt_gui=False):
    #show the plot
    if use_plt_gui:
        logging.debug('Showing plot...')
        fig.draw()#show(block=True)#False)
        fig.pause(1)#needed to give the script time to show the plot before asking for user input # plt.show(block=False)
        return None
    else:
        #save the plot then open it with PIL
        fig.savefig(paths_dict['interpolation_timeseries'])
        logging.debug('Saving plot at %s', paths_dict['interpolation_timeseries'])
        #open the plot
        im = Image.open(paths_dict['interpolation_timeseries'])
        im.show()
        return im                
        
def setup_interpolation_timeseries(index_row_no_year):
    #setup the plot but it will be added through the following code
    fig, ax = plt.subplots()
    #set up legend
    ax.legend()
    #set up title
    ax.set_title('Interpolation methods for {}'.format(index_row_no_year))
    #set up x axis label
    ax.set_xlabel('date')
    #slightly slant the x axis labels
    ax.tick_params(labelrotation=45)#TODO is this needing ax or fig
    #set up y axis label
    ax.set_ylabel('value')
    #set background as white
    ax.set_facecolor('white')
    logging.debug('Setup interpolation timeseries with index_row_no_year=%s', index_row_no_year)

    return fig, ax

def check_if_enough_values_to_interpolate(current_data,final_combined_data_concordance_measure, index_row_no_year,skipped_rows,INDEX_COLS_no_year,percent_of_values_needed_to_interpolate):
    """ sorry this function is a mess, one day I will clean it up"""
    ################################################################
    #if data is only one row long then we can't interpolate so we will skip it:
    if (len(current_data.shape) == 1) or current_data.shape[0] == 1:
        #if value is NaN then we can just set dataset_selection_method to 'not enough values to interpolate' in final_combined_data_concordance_measure, else we will leave it as it is
        skipped_rows.append(index_row_no_year)
        temp = final_combined_data_concordance_measure.loc[index_row_no_year]
        if current_data.shape[0] == 1:
            if np.isnan(temp.value[0]):
                #for some reason the shape is (1, 4) instead of (4,). this means we need to do it differently
                temp.dataset_selection_method ='not enough values to interpolate'
        elif np.isnan(temp.value):
                #for some reason the shape is (4,) instead of (1,4). this means we need to do it differently
                temp.dataset_selection_method ='not enough values to interpolate'
        final_combined_data_concordance_measure.loc[index_row_no_year] = temp
        # continue
        next_iteration = True
        return next_iteration,skipped_rows,current_data, final_combined_data_concordance_measure
    
    ################################################################
    #if tehre are no na values in the values column where dataset_selection_method is not == 'interpolation skipped' then we will not interpolate because there is no data to interpolate
    elif current_data.loc[current_data.dataset_selection_method != 'interpolation skipped', 'value'].isnull().sum() == 0:
        next_iteration = True
        return next_iteration,skipped_rows,current_data, final_combined_data_concordance_measure
    ################################################################
    #doulbe check that less than percent_of_values_needed_to_interpolate of the data is missing. If there is less vakues than this then we will skip it
    elif current_data['value'].isnull().sum() / len(current_data) > percent_of_values_needed_to_interpolate:
        skipped_rows.append(index_row_no_year)#we can check on this later to see if we should have interpolated these rows
        #also set dataset_selection_method where it is NA to 'not enough values to interpolate'
        current_data.loc[current_data['value'].isnull(), 'dataset_selection_method'] = 'not enough values to interpolate'
        #set final_combined_data_concordance_measure to the current data df (this will only change the dataset_selection_method column where the value is NA to indicate that we skipped it)
        #drop the index row from final_combined_data_concordance_measure
        final_combined_data_concordance_measure = final_combined_data_concordance_measure.drop(index_row_no_year)
        #then add the current data df to final_combined_data_concordance_measure
        final_combined_data_concordance_measure = pd.concat([final_combined_data_concordance_measure, current_data.loc[index_row_no_year]])  
        next_iteration = True
        return next_iteration,skipped_rows,current_data, final_combined_data_concordance_measure
    ################################################################
    #if we have made it this far then we will interpolate!
    else:
        next_iteration = False
        return next_iteration,skipped_rows,current_data, final_combined_data_concordance_measure

def apply_interpolation_method(user_input, interpolation_methods_current, current_data, index_row_no_year):
    interpolation_method = interpolation_methods_current[int(user_input)-1]
    #set interpolated value to that column and remove all interpoaltion columns
    current_data['interpolated_value'] = current_data[interpolation_method]
    dataset_selection_method = 'interpolation'
    current_data = current_data.drop(columns=interpolation_methods_current)
    user_input_correct = True
    return interpolation_method, dataset_selection_method, current_data, user_input_correct

def ask_user_to_choose_interpolation_method(final_combined_data_concordance_measure,im, interpolation_methods_current,current_data, index_row_no_year):
    logger = logging.getLogger(__name__)
    #ask the user to choose which method to use
    logger.info('{}: {}'.format('0', 'Skip this row'))
    for i in range(len(interpolation_methods_current)):
        logger.info('{}: {}'.format(i+1, interpolation_methods_current[i]))

    user_input_correct = False
    wrong_user_input = None
    user_input = None
    interpolation_method = None
    break_loop = False
    dataset_selection_method = None
    while user_input_correct == False:
        user_input = input('Choose interpolation method: ')
        if user_input == '0':
            user_input_correct = True#user skipped. 
            continue
        try:
            interpolation_method, dataset_selection_method, current_data, user_input_correct = apply_interpolation_method(user_input, interpolation_methods_current, current_data, index_row_no_year)
        except:
            if wrong_user_input == user_input:
                logger.error('You have input the same incorrect value twice. Quitting program to save progress')
                #need to quit the process but not create an error, so the stuff that has been done is saved
                break_loop = True
                user_input_correct = True#i think i can jsut rbeak the while loop too
                #sys.exit()#change this
            else:
                logger.error('Invalid input. Try again, if you input the same incorrect value again we will quit the program')
                wrong_user_input = user_input
    if user_input == '0':
        #if this happens then we will set the dataset_selection_method col to 'interpolation skipped'
        dataset_selection_method = 'interpolation skipped'
        #double check that that worked #TODO
        logger.debug('dataset_selection_method for {} is {}'.format(index_row_no_year, final_combined_data_concordance_measure.loc[index_row_no_year, 'dataset_selection_method']))

    return final_combined_data_concordance_measure,interpolation_method, dataset_selection_method, current_data, user_input_correct, break_loop, im
########################MANUAL

def do_automatic_interpolation(current_data, INTERPOLATION_LIMIT,index_row_no_year,automatic_interpolation_method):
    #set the interpolation method to whatever was determined as the best method before.
    interpolation_method = automatic_interpolation_method
    dataset_selection_method= None
    #if the method is one that requires an order then it will have  a number as well so check if the value has a number in it
    if re.search(r'\d', interpolation_method):
        order = int(re.search(r'\d', interpolation_method).group())
        interpolation_method_string = re.sub(r'\d', '', interpolation_method)

        try:
            #interpolate the values for the interpolation method
            current_data['interpolated_value'] = current_data['value'].interpolate(method=interpolation_method_string, order=order, limit_direction='both', limit=INTERPOLATION_LIMIT)
            dataset_selection_method = 'interpolation'
        except:
            #spline method order is too high, try lower order and if that fails then try linear method
            interpolated = False
            for order in range(1, order, -1):
                try:
                    #interpolate the values for the interpolation method
                    current_data['interpolated_value'] = current_data['value'].interpolate(method=interpolation_method_string, order=order, limit_direction='both', limit=INTERPOLATION_LIMIT)
                    interpolated = True
                    dataset_selection_method = 'interpolation'
                    break
                except:
                    continue

            #if the spline method failed then try the linear method
            if interpolated == False:
                try:#this shouldnt fail but just in case
                    interpolation_method_string = 'linear'
                    #interpolate the values for the interpolation method
                    current_data['interpolated_value'] = current_data['value'].interpolate(method=interpolation_method_string, limit_direction='both', limit=INTERPOLATION_LIMIT)
                    dataset_selection_method = 'interpolation'
                except:
                    #if the linear method fails then set the dataset_selection_method to 'interpolation skipped'
                    dataset_selection_method = 'interpolation skipped'
                    logging.warning('interpolation failed for %s, so it has been recorded as "interpolation skipped"', index_row_no_year)
    else:   
        #interpolate the values for the interpolation method that doesnt    require an order
        try:#this shouldnt fail but just in case
            interpolation_method_string = interpolation_method
            #interpolate the values for the interpolation method
            current_data['interpolated_value'] = current_data['value'].interpolate(method=interpolation_method_string, limit_direction='both', limit=INTERPOLATION_LIMIT)
            dataset_selection_method = 'interpolation'
        except:
            #if the linear method fails then set the dataset_selection_method to 'interpolation skipped'
            dataset_selection_method = 'interpolation skipped'
            logging.warning('interpolation failed for %s, so it has been recorded as "interpolation skipped"', index_row_no_year)

    return current_data, dataset_selection_method

def separate_dataset_and_source(final_combined_data_concordance):

    #and just quickly separate the dataset and source coplumns by splitting the value on any $ signs
    final_combined_data_concordance['source'] = final_combined_data_concordance['dataset'].str.split('$').str[1]
    #remove any spaces
    final_combined_data_concordance['source'] = final_combined_data_concordance['source'].str.strip()

    final_combined_data_concordance['dataset'] = final_combined_data_concordance['dataset'].str.split('$').str[0]
    #remove any spaces
    final_combined_data_concordance['dataset'] = final_combined_data_concordance['dataset'].str.strip()
    return final_combined_data_concordance



################################################################################################################################################################



def finalise_interpolation(current_data, dataset_selection_method, final_combined_data_concordance_measure, index_row_no_year, INDEX_COLS_no_year):
    current_data.loc[current_data['value'].isna(), 'dataset_selection_method'] = dataset_selection_method

    if dataset_selection_method == 'interpolation':
        current_data.loc[current_data['value'].isna() & current_data['interpolated_value'].notna(), 'value'] = current_data['interpolated_value']
        # testing
        # check if there are any values where interpolated_value is nan as well as value. this doesn't seem to occur but best to be safe
        if current_data.loc[current_data['value'].isna() & current_data['interpolated_value'].isna()].shape[0] > 0:
            logging.warning('There are values where interpolated_value is nan as well as value')
            logging.warning(current_data.loc[current_data['value'].isna() & current_data['interpolated_value'].isna()])
            # set the dataset_selection_method to 'interpolation skipped' for this specific row:
            current_data.loc[current_data['value'].isna() & current_data['interpolated_value'].isna(), 'dataset_selection_method'] = 'interpolation skipped'
            # throw error
            # raise ValueError('There are values where interpolated_value is nan as well as value')#this did happen once when I was loading in progress not related to the new data I was interpolating
    elif dataset_selection_method == 'interpolation skipped':
        pass
    else:
        logging.error('Something is wrong with the dataset_selection_method for {}'.format(index_row_no_year))
        # throw error
        raise ValueError('Something is wrong with the dataset_selection_method for {}'.format(index_row_no_year))

    # set the index to the original index#.reset_index()
    current_data = current_data.set_index(INDEX_COLS_no_year)
    # all done with the current index row, so set the data for the current index row to the current_data dataframe
    # drop the index row from final_combined_data_concordance_measure
    final_combined_data_concordance_measure = final_combined_data_concordance_measure.drop(index_row_no_year)
    # then add the current data df to final_combined_data_concordance_measure
    final_combined_data_concordance_measure = pd.concat([final_combined_data_concordance_measure, current_data.loc[index_row_no_year]])

    return final_combined_data_concordance_measure




# def load_interpolation_progress():
#         ########################################################
#         print('Starting interpolation, this may take a few minutes...')
#         #load progress#todo this needs to be a lot smoother
#         if load_progress:
#             filename ='intermediate_data/interpolation/{}_progress.csv'.format(FILE_DATE_ID)
#             progress = pd.read_csv(filename)
#             #set index rows
#             progress = progress.set_index(INDEX_COLS_no_year)
#             final_combined_data_concordance = final_combined_data_concordance.set_index(INDEX_COLS_no_year)
#             #check for any cols called 'Unnamed' and remove them (this error occurs often here so we will just remove them if they exist)
#             progress = progress.loc[:, ~progress.columns.str.contains('Unnamed')]
#             print('Unnamed cols removed from progress dataframe in interpolation')
#             # in case the user wants to stop the code running and come back to it later, we will save the data after each measure is done. This will allow the user to pick up where they left off. So here we will load in the data from the last saved file and update the final_combined_data_concordance df so any of the users changes are kept. This will also identify if the data currently set in combined data concordance for that index row has changed since last time. If it has then we will not overwrite it an the user will ahve to inrerpolate it again.
#             #so first identify where there are either 'interpolation' or 'interpolation skipped' in the dataset_selection_method column:
#             previous_progress = progress.loc[progress.dataset_selection_method.isin(['interpolation', 'interpolation skipped'])]
#             previous_progress_no_interpolation = progress.loc[~progress.dataset_selection_method.isin(['interpolation', 'interpolation skipped'])]
#             #for each index row in previous_progress, check if teh sum of value col in previous_progress_no_interpolation, matches that for final_combined_data_concordance. If not then some data point has changed and we will need to reinterpolate the data, otherwiose we can just replace the data in final_combined_data_concordance with the data in previous_progress
#             for index_row_no_year in previous_progress_no_interpolation.index.unique():
#                 if index_row_no_year in final_combined_data_concordance.index.unique():
#                     #calc sum of values for index_row_no_year
#                     previous_progress_no_interpolation_value_sum = previous_progress_no_interpolation.loc[index_row_no_year].value.sum()
#                     final_combined_data_concordance_value_sum = final_combined_data_concordance.loc[index_row_no_year].value.sum()

#                     if previous_progress_no_interpolation_value_sum != final_combined_data_concordance_value_sum:
#                         #if the values are different then we need to reinterpolate the data (so leave the data in final_combined_data_concordance as it is)
#                         # previous_progress = previous_progress.drop(index=index_row_no_year)
#                         # print('The data for {} has changed since last time, so the interpolation will need to be redone'.format(index_row_no_year))
#                         pass

#                     else:
#                         #if vlkauyes are the same we want to replace all rows for this index row in final_combined_data_concordance with the rows in progress so we can pick up where we left off
#                         final_combined_data_concordance = final_combined_data_concordance.drop(index=index_row_no_year)
#                         final_combined_data_concordance = pd.concat([final_combined_data_concordance, progress.loc[index_row_no_year]])

#             #reset index
#             final_combined_data_concordance = final_combined_data_concordance.reset_index()
#             print('Time taken so far: {}'.format(datetime.datetime.now() - start_time))
#             print('Previous progress loaded, interpolation will now start')
#             ########################################################
        