include "/~ab_home/include/analysis-extensions.dml";
 
///////////////////////////////////////////////////////////////////////////////////////
//
//      Check to see that a max-core/max-memory parameter is set to $AI_SORT_MAX_CORE
//
///////////////////////////////////////////////////////////////////////////////////////
 
vld_simple_result_t out :: check_for_maxcore_maxmemory(validation_parameter_t param) =
begin
  let  flag = 0 ;
  let match_vec = [vector '${AI_SORT_MAX_CORE}', '$AI_SORT_MAX_CORE', '$AI_GRAPH_MAX_CORE_MIN', '${AI_GRAPH_MAX_CORE_MIN}', '$AI_GRAPH_MAX_CORE_HALF', '${AI_GRAPH_MAX_CORE_HALF}' ];
 
   flag = if(((param.owner_type member [vector 'Sort.mpc' , 'Sort_within_Groups.mpc', 'Rollup.mpc', 'Scan.mpc', 'Scan_with_Rollup.mpc', 'Join.mpc', 'Subscribe_to_SAP_Extractor.mpc']
                and (param.name member [vector 'max-core', 'max_memory', 'max_core' ,'max_memory']))) and
                          not (param.unresolved_value member match_vec)) 1
                          
           else 0;                       
   if(!flag)
        exit check_for_maxcore_maxmemory;
 
   out.message :: if(flag==1) string_concat('  For ',string_replace(param.owner_type,".mpc",""),' component, set ',param.name, ' parameter value to' , string_join(match_vec, ' or '),  'based on requirement');
end;
 
/////////////////////TO CHECK IF REFORMAT COMPONENENTS ARE REPEATING OR REFORMAT AFTER NORMALIZE COMPONENTS ///////////           
 
 
vld_simple_result_t out :: check_reformats(validation_flow_t flow)=
begin
 
  let problem = 0;
  if ((flow.source_component_type member [ vector "Reformat.mpc"] ) and
      (flow.target_component_type member [ vector "Reformat.mpc", "Normalize.mpc"]))
      problem = 1;
  if (!problem)
      exit check_reformats;
 
out.message :: " WARNING : Multiple Reformats or Normalize component followed by Reformat component are connected. Please remove unnecessary components " ;
end;
 
 
/////////////////////Check No of Phases in Graph and if execeeds 3 , issue a Warning Message///////////
vld_simple_result_t out :: find_the_no_of_phases(validation_component_t comp) =
begin
     
        let decimal("") threshold = 4;
        let decimal("") phase_num = if(comp.component_type member [vector "Output_File.mdc","Ouput_Table.mdc" , "Run_Program.mpc" ,"Write_Multiple_Files.mpc"]) (decimal(""))(string(""))comp.phase +1
                        else 1;
       
        
        if( phase_num < threshold)
            exit find_the_no_of_phases;
                  
 
out.message ::  string_concat ( "  Warning : There are " , phase_num , " number of Phases in the graph, Reduce to enhance Graph Performance, unless the graph is using more memory in each phase ");
                
                
end;
 
 
/////////////////////TO CHECK FOR EMBEDDED OUTPUT TABLE DML///////////
vld_simple_result_t out :: check_for_embedded_file_dml (validation_parameter_t param) =
begin
  let long bad = if((param.owner_type member [vector 'Output_File.mdc', 'Input_File.mdc']) and (param.name member [vector 'write_metadata','read_metadata']) and (param.is_embedded==1)
                        and (!string_like(param.value, "%metadata%") and !starts_with(param.unresolved_value, "$")  and length_of(string_split_no_empty(param.unresolved_value, '\\s')) != 1 ) )
                        1
                 else   0;
 
                 if (!bad)
                 exit check_for_embedded_file_dml;
 
  out.message :: "  Warning:: Input or Output Dataset DML is embedded, Please check and correct if needed  ";
 
end;
 
 
//////////////////TO CHECK REOFORMAT TRANSFORM EMBEDDED OR NOT///////////
vld_simple_result_t out :: check_for_tf_embedded (validation_parameter_t param) =
begin
  let long bad = if((param.owner_type member [vector 'Reformat.mpc', 'Multi_Reformat.mpc' , 'Join.mpc','Rollup.mpc','Scan.mpc','Normalize.mpc']) and
                                      (starts_with(param.name,'transform')) and
                                                       (param.is_embedded==1 and !is_blank(param.unresolved_value))) 1 else 0;
 
  if ( !bad)
    exit check_for_tf_embedded;
 
  out.message :: string_concat ( "  Transform in " , param.owner_full_name , " shouldn't be embedded , Please parameterize " );
 
end;
 
 
 
////////////////// To Check intermedate file using or not//////////////
vld_simple_result_t out :: check_for_inter_file_using
    (validation_component_t param) =
begin
  let long bad = if((param.component_type =="Intermediate_File.mdc")) 1 else 0;
 
  if ( !bad)
 
  
    exit check_for_inter_file_using;
 
  out.message :: "  Intermediate file is being used in graph , If there is a strong business case then keep it.  ";
end;
 
 
 ////////////////// To check for Run Program Component and ensure ksh -c -e is present in the first line//////////
vld_simple_result_t out :: check_options_for_run_program
    (validation_parameter_t param) =
begin
  let long bad = if(param.owner_type =="Run_Program.mpc" and starts_with(param.name, 'commandline') and !starts_with(param.value, 'ksh -c -e')) 1 else 0;
 
  if ( !bad)
    exit check_options_for_run_program;
 
  out.message :: "  In Run Program component, the first line should start with \"ksh -c -e\" Shell will exit wth non-zero status ";
end;
 
 
 
 
