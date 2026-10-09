/*
Use this procedure to execute
 - a function
 - a procedure
 - a statement
with a lock timeout. Any lock taking longer than v_detach_lock_timeout_ms (default 1000ms) will cause the script
to release the lock and try again after v_detach_retry_sleep_sec (default 20s).

It is possible to only run the procedure between given timestamps (with timezone) or with a maximum number of retries.

N.B.
The input is executed under the caller's search_path, so schema-qualify the objects it references.

    PARAMETER                   TYPE                                DESCRIPTION
    v_input                     TEXT                                input to execute
    v_arguments                 TEXT DEFAULT NULL                   argument for a function or procedure
    v_detach_lock_timeout_ms    INT DEFAULT 1000                    maximum time in ms to try to get locks
    v_detach_retry_sleep_sec    INT DEFAULT 20                      time in seconds between execution attempts
    v_max_retries               INT DEFAULT NULL                    maximum number of attempts to execute the input
    v_time_start                TIME WITH TIME ZONE DEFAULT NULL    starting time of the first execution attempt
    v_time_end                  TIME WITH TIME ZONE DEFAULT NULL    end time to execute the input

Examples:
    call dba.lock_safe_execute('lock_test_with_arguments', '''arg1'', 1', 2000, 10, 10, TIME '10:00', null);

    call dba.lock_safe_execute('lock_test', v_time_start => TIME '10:00', v_time_end => TIME '16:00');

    call dba.lock_safe_execute('procedure_name', v_time_end => TIME '16:00');

    call dba.lock_safe_execute('alter table test add column c1 int', v_max_retries => 50);
*/

CREATE OR REPLACE PROCEDURE dba.lock_safe_execute(v_input text, v_arguments text default null, v_detach_lock_timeout_ms int default 1000, v_detach_retry_sleep_sec int default 20, v_max_retries int default null, v_time_start time with time zone default null, v_time_end time with time zone default null)
language plpgsql
AS $proc$
DECLARE
	v_type CHAR;
	statement TEXT;
	loop_count INT := 0;
	v_name TEXT;

BEGIN

    IF (v_input is null) THEN
        RAISE EXCEPTION 'Input for execution can not be null';
    END IF;

    -- Remove schema name if given
    IF pg_catalog.split_part(v_input, '.', 2) = '' THEN
        v_name := pg_catalog.split_part(v_input, '.', 1) ;
    ELSE
        v_name := pg_catalog.split_part(v_input, '.', 2) ;
    END IF;

    -- function, procedure or single statement
    EXECUTE pg_catalog.format($sql$ select prokind from pg_catalog.pg_proc where proname = %L $sql$, v_name) INTO v_type;

    CASE v_type
    	WHEN 'f' THEN
    		RAISE DEBUG 'executing function';
    		statement := pg_catalog.concat('select ', v_input, '(', v_arguments, ')');
		WHEN 'p' THEN
			RAISE DEBUG 'execution procedure';
			statement := pg_catalog.concat('call ', v_input, '(', v_arguments, ')');
		ELSE
			RAISE DEBUG 'executing statement';
			statement := v_input;
	END CASE;

    -- Check moment of first execution
	IF ( v_time_start > pg_catalog.clock_timestamp()::time ) THEN
		RAISE DEBUG 'Sleep until % to start execution', v_time_start;
		PERFORM pg_catalog.pg_sleep_until(current_date + v_time_start);
	END IF;

	RAISE NOTICE 'executing: % at %', statement, pg_catalog.clock_timestamp();

    /*
    Loop until
        - execution succeeds
        - the number of retries has been reached
        - v_time_end is reached and v_time_start is not specified
    */
	WHILE TRUE LOOP
	    -- Re-apply lock timeout each iteration; savepoint rollback on lock_not_available resets it
	    EXECUTE pg_catalog.format('SET local lock_timeout TO %s', v_detach_lock_timeout_ms);
	    BEGIN
            -- Execute the function/procedure/statement
	    	EXECUTE statement;

    		-- Executed the given input. Exit the loop.
	    	EXIT;

	        EXCEPTION
	            -- Lock not available within specified time
	            WHEN lock_not_available THEN
					loop_count := loop_count +1;
					RAISE NOTICE 'lock not available %', loop_count;

					-- Check for the maximum number of executions
					IF (loop_count >= v_max_retries) THEN
						RAISE EXCEPTION 'Failed to execute % within % attempts', statement, v_max_retries USING ERRCODE = '01501';
					END IF;

                    -- Sleep until for specified number of seconds
	                PERFORM pg_catalog.pg_sleep(v_detach_retry_sleep_sec);

	                -- Check the next attempt is within the specified interval
					IF (pg_catalog.clock_timestamp()::time > v_time_end) THEN
					    -- We are after the specified interval
						IF (v_time_start is null) THEN
						    -- No start time has been provide. We stop here.
							RAISE EXCEPTION 'Failed to execute % in time', statement USING ERRCODE = '01502';
						END IF;

                        -- Next execution is on the next day at v_time_start
						RAISE NOTICE 'End of specified time interval for execution. Continue at %', pg_catalog.clock_timestamp()::date + v_time_start  + '1 day'::interval;
						PERFORM pg_catalog.pg_sleep_until(pg_catalog.clock_timestamp()::date + v_time_start + '1 day'::interval);
					END IF;
                WHEN OTHERS THEN
                    RAISE EXCEPTION 'Unexpected exception. Message: %, Code: %', SQLERRM, SQLSTATE USING ERRCODE = '45001';
	    END;
	END LOOP;
END;
$proc$;
