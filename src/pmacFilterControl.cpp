// An application to listen on a ZMQ channel for histograms and adjust a filter set

#include <iostream>  // std::cout
#include <functional>  // std::bind
#include <time.h>  // timespec, CLOCK_REALTIME
#include <sstream>  // std::stringstream, std::getline

#include "pmacFilterControl.h"
#include "version.h"

#ifdef __ARM_ARCH
#include "gplib.h"  // pshm, CommandTS
#endif

const int MAX_ATTENUATION = 15;  // All filters in: 1 + 2 + 4 + 8
const long POLL_TIMEOUT = 100;  // Length of ZMQ poll in milliseconds
const int FILTER_COUNT = 4;  // Number of filters

// Command to send to motion controller to execute the motion program and move to the set demands
char RUN_PROG_1[] = "&2 #1,2,3,4J/ B1R";

const std::string CLOSE_SHUTTER_STR = "#5J=";

// Control message keys
const std::string COMMAND = "command";
const std::string COMMAND_SHUTDOWN = "shutdown";
const std::string COMMAND_STATUS = "status";
const std::string COMMAND_CONFIGURE = "configure";
const std::string COMMAND_RESET = "reset";
const std::string COMMAND_CLEAR_ERROR = "clear_error";
const std::string COMMAND_SINGLESHOT_START = "singleshot";
const std::string PARAMS = "params";
const std::string CONFIG_MODE = "mode";  // Values defined by ControlMode
const std::string CONFIG_IN_POSITIONS = "in_positions";
const std::string CONFIG_OUT_POSITIONS = "out_positions";
const std::string CONFIG_SHUTTER_CLOSED_POSITION = "shutter_closed_position";
const std::string CONFIG_PIXEL_COUNT_THRESHOLDS = "pixel_count_thresholds";
const std::string CONFIG_ATTENUATION = "attenuation";
const std::string CONFIG_TIMEOUT = "timeout";
const std::string FILTER_1_KEY = "filter1";
const std::string FILTER_2_KEY = "filter2";
const std::string FILTER_3_KEY = "filter3";
const std::string FILTER_4_KEY = "filter4";
const std::map<std::string, int> FILTER_MAP = {
    {FILTER_1_KEY, 0},
    {FILTER_2_KEY, 1},
    {FILTER_3_KEY, 2},
    {FILTER_4_KEY, 3}
};

// Data message keys
const std::string FRAME_NUMBER = "frame_number";
const std::string PARAMETERS = "parameters";
const std::string PARAM_LOW1 = "low1";
const std::string PARAM_LOW2 = "low2";
const std::string PARAM_HIGH1 = "high1";
const std::string PARAM_HIGH2 = "high2";
const std::string PARAM_HIGH3 = "high3";

// Event message keys
const std::string ADJUSTMENT = "adjustment";
const std::string ATTENUATION = "attenuation";

// The attenuation adjustments to apply for a given threshold
// PARAM_HIGH3 -> Max attenuation
// PARAM_HIGH2 -> Add 2 levels of attenuation
// PARAM_HIGH1 -> Add 1 level of attenuation
// PARAM_LOW1 -> Subtract 1 level of attenuation
// PARAM_LOW2 -> Subtract 2 levels of attenuation
const std::map<std::string, int> THRESHOLD_ADJUSTMENTS = {
    {PARAM_HIGH3, 15}, {PARAM_HIGH2, 2}, {PARAM_HIGH1, 1}, {PARAM_LOW2, -2}, {PARAM_LOW1, -1}
};

// An initial invalid value to compare with `last_processed_frame_` that always passes the ignore frame checks
const int64_t NO_FRAMES_PROCESSED = -2;


/*!
    @brief Constructor

    Setup ZeroMQ sockets

    @param[in] control_port Port number to bind control socket to
    @param[in] publish_port Port number to bind event stream publish socket to
    @param[in] subscribe_endpoints Vector of endpoints (`IP`:`PORT`) to subscribe on for data messages
*/
PMACFilterController::PMACFilterController(
    const std::string& control_port,
    const std::string& publish_port,
    const std::vector<std::string>& subscribe_endpoints
) :
    control_channel_endpoint_("tcp://*:" + control_port),
    publish_channel_endpoint_("tcp://*:" + publish_port),
    subscribe_channel_endpoints_(subscribe_endpoints),
    zmq_context_(),
    zmq_control_socket_(zmq_context_, ZMQ_REP),
    zmq_publish_socket_(zmq_context_, ZMQ_PUB),
    zmq_subscribe_sockets_(),
    // Internal logic
    state_(ControlState::IDLE),
    last_received_frame_(NO_FRAMES_PROCESSED),
    last_processed_frame_(NO_FRAMES_PROCESSED),
    last_message_ts_(),
    last_process_ts_(),
    process_duration_(0),
    process_period_(0),
    singleshot_start_(false),
    shutter_close_string_("#5J=1000"),
    clear_error_(false),
    shutdown_(false),
    // Filter logic
    current_attenuation_(0),
    last_adjustment_(0),
    current_demand_(FILTER_COUNT, 0),
    post_in_demand_(FILTER_COUNT, 0),
    final_demand_(FILTER_COUNT, 0),
    move_duration_(0),
    // Default config parameter values
    mode_(ControlMode::MANUAL),
    timeout_(3.0),
    in_positions_({100, 100, 100, 100}),
    out_positions_({0, 0, 0, 0}),
    pixel_count_thresholds_({{PARAM_LOW1, 2}, {PARAM_LOW2, 2}, {PARAM_HIGH1, 2}, {PARAM_HIGH2, 2}, {PARAM_HIGH3, 2}})
{
    this->zmq_control_socket_.bind(control_channel_endpoint_.c_str());
    this->zmq_publish_socket_.bind(publish_channel_endpoint_.c_str());

    // Open sockets to subscribe to data endpoints
    for (size_t idx = 0; idx != this->subscribe_channel_endpoints_.size(); idx++) {
        this->zmq_subscribe_sockets_.push_back(zmq::socket_t(this->zmq_context_, ZMQ_SUB));
        // Only recv most recent message
        const int conflate = 1;
        this->zmq_subscribe_sockets_[idx].setsockopt(ZMQ_CONFLATE, &conflate, sizeof(conflate));
        // Subscribe to all topics ("" -> No topic filter)
        this->zmq_subscribe_sockets_[idx].setsockopt(ZMQ_SUBSCRIBE, "", 0);
        this->zmq_subscribe_sockets_[idx].connect(this->subscribe_channel_endpoints_[idx].c_str());
    }
}

/*!
    @brief Destructor

    Close ZeroMQ sockets

*/
PMACFilterController::~PMACFilterController() {
    this->zmq_control_socket_.close();
    this->zmq_publish_socket_.close();
    std::vector<zmq::socket_t>::iterator it;
    for (it = this->zmq_subscribe_sockets_.begin(); it != this->zmq_subscribe_sockets_.end(); it++) {
        it->close();
    }
}

/*!
    @brief Handle the JSON request from the control channel

    This method assumes request has contains a `COMMAND` field and accesses it without checking.

    @throw Assertion failed if there is no `COMMAND` field

    @param[in] request json object of request
    @param[out] response json object to add response to

    @return true if the request was applied successfully, else false
*/
bool PMACFilterController::_handle_request(const json& request, json& response) {
    if (!_is_valid_request(request)) {
        return false;
    }

    bool success = true;
    if (request[COMMAND] == COMMAND_SHUTDOWN) {
        std::cout << "Received shutdown command" << std::endl;
        this->shutdown_ = true;
        success = true;
    } else if (request[COMMAND] == COMMAND_RESET) {
        std::cout << "Resetting frame counter" << std::endl;
        this->last_received_frame_ = NO_FRAMES_PROCESSED;
        this->last_processed_frame_ = NO_FRAMES_PROCESSED;
        success = true;
    } else if (request[COMMAND] == COMMAND_CLEAR_ERROR) {
        this->clear_error_ = true;
    } else if (request[COMMAND] == COMMAND_SINGLESHOT_START) {
        this->singleshot_start_ = true;
        success = true;
    } else if (request[COMMAND] == COMMAND_STATUS) {
        this->_handle_status(response);
        success = true;
    } else if (request[COMMAND] == COMMAND_CONFIGURE) {
        if (request.contains(PARAMS)) {
            json config = request[PARAMS];
            std::cout << "Received new config: " << config.dump() << std::endl;
            try {
                success = this->_handle_config(config);
            } catch (json::type_error& e) {
                std::cout << "Type error when handling config" << std::endl;
                success = false;
            }
        } else {
            std::cout << "Received config command with no parameters" << std::endl;
            success = false;
        }
    } else {
        std::cout << "Invalid command" << std::endl;
        success = false;
    }

    return success;
}

/*!
    @brief Handle a configuration request

    @param[in] config json object of config parameters

    @throw json::type_error if given a config parameter with the wrong type

    @return true if all given parameters applied successfully, false if one or more failed or no parameters found
*/
bool PMACFilterController::_handle_config(const json& config) {
    bool success = false;

    if (config.contains(CONFIG_MODE)) {
        success = this->_set_mode(config[CONFIG_MODE]);
    }
    if (config.contains(CONFIG_IN_POSITIONS)) {
        success = this->_set_positions(this->in_positions_, config[CONFIG_IN_POSITIONS]);
    }
    if (config.contains(CONFIG_OUT_POSITIONS)) {
        success = this->_set_positions(this->out_positions_, config[CONFIG_OUT_POSITIONS]);
    }
    if (config.contains(CONFIG_SHUTTER_CLOSED_POSITION)) {
        success = this->_set_shutter_closed_position(config[CONFIG_SHUTTER_CLOSED_POSITION]);
    }
    if (config.contains(CONFIG_PIXEL_COUNT_THRESHOLDS)) {
        success = this->_set_pixel_count_thresholds(config[CONFIG_PIXEL_COUNT_THRESHOLDS]);
    }
    if (config.contains(CONFIG_ATTENUATION)) {
        if (this->mode_ == ControlMode::MANUAL) {
            this->_set_attenuation(config[CONFIG_ATTENUATION]);
            success = true;
        } else {
            std::cout << "Can only set attenuation in MANUAL mode" << std::endl;
            success = false;
        }
    }
    if (config.contains(CONFIG_TIMEOUT)) {
        success = this->_set_timeout(config[CONFIG_TIMEOUT]);
    }

    if (!success) {
        std::cout << "Given configuration failed or found no valid config parameters" << std::endl;
    }

    return success;
}

/*!
    @brief Set the mode enum with value checking

    @param[in] mode ControlMode (enum value) of mode to set

    @return true if the mode was set successfully, else false
*/
bool PMACFilterController::_set_mode(ControlMode mode) {
    bool success = true;

    std::cout << "Changing to mode " << mode << std::endl;

    if (mode < ControlMode::CONTROL_MODE_SIZE) {
        this->mode_ = mode;
    } else {
        std::cout << "Unknown mode: " << mode <<
            ". Allowed modes: 0 - " << ControlMode::CONTROL_MODE_SIZE - 1 << std::endl;
        success = false;
    }

    return success;
}

/*!
    @brief Update timeout config variable from given value

    @param[in] timeout Timeout value in seconds

    @return true if the mode was set successfully, else false
*/
bool PMACFilterController::_set_timeout(float timeout) {
    bool success = true;

    if (timeout < 0.0) {
        std::cout << "Timeout must be >= 0.0 (seconds)" << std::endl;
        success = false;
    }
    else {
        std::cout << "Changing timeout to " << timeout << " seconds" << std::endl;
        this->timeout_ = timeout;
    }
    return success;
}

/*!
    @brief Update the given positions vector from given new values

    @param[in] positions active filter positions vector - either `this->in_positions_` or `this->out_positions_`
    @param[in] new_positions json dictionary of new filter positions - e.g. {"filter1": 100, "filter2": -100, ...}

    @throw json::type_error if given a config parameter with the wrong type

    @return true if at least one position was set, else false
*/
bool PMACFilterController::_set_positions(std::vector<int>& positions, json new_positions) {
    bool success = false;

    std::map<std::string, int>::const_iterator item;
    for(item = FILTER_MAP.begin(); item != FILTER_MAP.end(); ++item) {
        if (new_positions.contains(item->first)) {
            positions[item->second] = new_positions[item->first];
            success = true;
        }
    }

    return success;
}

/**
 * @brief Update the given shutter closed position from the given value
 * 
 * @param[in] shutter_closed_position shutter closed position
 * 
 * @return true if the position was set, else false
 */
bool PMACFilterController::_set_shutter_closed_position(int shutter_closed_position) {
    bool success = true;

    std::string shutter_close_string = "";

    shutter_close_string = CLOSE_SHUTTER_STR + std::to_string(shutter_closed_position);

    this->shutter_close_string_.assign(shutter_close_string);

    return success;
}

/*!
    @brief Set the in pixel count thresholds of the given histogram bins

    @param[in] thresholds json dictionary of pixel count thresholds - e.g. {"high1": 100, "low2": 500, ...}

    @throw json::type_error if given a config parameter with the wrong type

    @return true if at least one threshold was set, else false
*/
bool PMACFilterController::_set_pixel_count_thresholds(json thresholds) {
    bool success = false;

    std::map<std::string, int>::const_iterator it;
    for(it = THRESHOLD_ADJUSTMENTS.begin(); it != THRESHOLD_ADJUSTMENTS.end(); ++it) {
        if (thresholds.contains(it->first)) {
            this->pixel_count_thresholds_[it->first] = thresholds[it->first];
            success = true;
        }
    }

    return success;
}

/*!
    @brief Handle a status request from the control channel

    @param[out] response json object to add status to
*/
void PMACFilterController::_handle_status(json& response) {
    json status;
    // Read-only status items
    status["version"] = VERSION;
    status["process_duration"] = this->process_duration_;
    status["process_period"] = this->process_period_;
    status["move_duration"] = this->move_duration_;
    status["last_received_frame"] = this->last_received_frame_;
    status["last_processed_frame"] = this->last_processed_frame_;
    status["time_since_last_message"] = _seconds_since(this->last_message_ts_);
    status["current_attenuation"] = this->current_attenuation_;
    status["timeout"] = this->timeout_;
    status["state"] = this->state_;
    // Readback values for config items
    status[CONFIG_MODE] = this->mode_;
    status[CONFIG_IN_POSITIONS] = this->in_positions_;
    status[CONFIG_OUT_POSITIONS] = this->out_positions_;
    status[CONFIG_PIXEL_COUNT_THRESHOLDS] = this->pixel_count_thresholds_;

    response[COMMAND_STATUS] = status;
}

/*!
    @brief Spawn data monitor thread and listen for control requests until shutdown
*/
void PMACFilterController::run() {
    // Start data handler and event stream threads
    this->subscribe_thread_ = std::thread(
        std::bind(&PMACFilterController::_process_data_channel, this)
    );

    // Listen for control messages
    std::string request_str;
    while (!this->shutdown_) {
        zmq::message_t request_msg;
        this->zmq_control_socket_.recv(&request_msg);
        request_str = std::string(static_cast<char*>(request_msg.data()), request_msg.size());

        json request, response;
        request = _parse_json_string(request_str);
        response["success"] = this->_handle_request(request, response);

        std::string response_str = response.dump();
        zmq::message_t response_msg(response_str.size());
        memcpy(response_msg.data(), response_str.c_str(), response_str.size());
        this->zmq_control_socket_.send(response_msg, 0);
    }

    std::cout << "Shutting down" << std::endl;

    this->subscribe_thread_.join();
}

/*!
    @brief Listen on ZeroMQ data channel for messages and hand off for processing

    This method should be run in a spawned thread and will return when `shutdown_`
    is set to `true`
*/
void PMACFilterController::_process_data_channel() {
    // Construct pollitems for data sockets
    zmq::pollitem_t pollitems[this->zmq_subscribe_sockets_.size()];
    for (size_t idx = 0; idx != this->zmq_subscribe_sockets_.size(); idx++) {
        zmq::pollitem_t pollitem = {this->zmq_subscribe_sockets_[idx], 0, ZMQ_POLLIN, 0};
        pollitems[idx] = pollitem;
    }

    std::cout << "Listening for messages..." << std::endl;

    std::string data_str;
    struct timespec process_start_ts;
    while (!this->shutdown_) {
        this->_process_state_changes();

        // Poll data sockets
        zmq::poll(&pollitems[0], this->zmq_subscribe_sockets_.size(), POLL_TIMEOUT);
        for (size_t idx = 0; idx != this->zmq_subscribe_sockets_.size(); idx++) {
            if (_message_queued(pollitems[idx])) {
                _get_time(&process_start_ts);

                zmq::message_t data_message;
                this->zmq_subscribe_sockets_[idx].recv(&data_message);

                if (!(this->state_ == ControlState::WAITING || this->state_ == ControlState::ACTIVE)) {
                    // Receive and ignore messages to keep the sockets clear
                    continue;
                }

                this->_handle_data_message(data_message);

                this->process_duration_ = (this->process_duration_ + _useconds_since(process_start_ts)) / 2;

                if (this->state_ == ControlState::WAITING) {
                    // Change from waiting to active to enable timeout monitoring
                    this->_transition_state(ControlState::ACTIVE);
                }
            }
        }
    }
}

/*!
    @brief Update state based on mode changes from control thread and internal logic
*/
void PMACFilterController::_process_state_changes() {
    // Manual mode disables monitoring of data channel
    if (this->mode_ == ControlMode::MANUAL) {
        this->_transition_state(ControlState::IDLE);
    }

    // Transition state to waiting depending on mode change
    if (this->mode_ == ControlMode::CONTINUOUS) {
        if (this->state_ == ControlState::IDLE ||
            this->state_ == ControlState::SINGLESHOT_COMPLETE ||
            this->state_ == ControlState::SINGLESHOT_WAITING
        ) {
            this->_transition_state(ControlState::WAITING);
        }
    } else if (this->mode_ == ControlMode::SINGLESHOT) {
        if (this->state_ == ControlState::IDLE || this->state_ == ControlState::WAITING) {
            this->_transition_state(ControlState::SINGLESHOT_WAITING);
        }
        this->_process_singleshot_state();
    }

    // Set max attenuation and stop if timeout reached
    if (
        (this->state_ == ControlState::ACTIVE || this->state_ == ControlState::SINGLESHOT_COMPLETE) &&
         _seconds_since(this->last_message_ts_) >= this->timeout_
    ) {
        std::cout << "Timeout waiting for messages" << std::endl;
        this->_transition_state(ControlState::TIMEOUT);
    }
    // Clear timeout if requested from control thread
    else if (this->state_ < 0 && this->clear_error_) {
        std::cout << "Error cleared - waiting for messages" << std::endl;
        this->clear_error_ = false;
        if (this->mode_ == ControlMode::SINGLESHOT) {
            this->_transition_state(ControlState::SINGLESHOT_WAITING);
        }
        else {
            this->_transition_state(ControlState::WAITING);
        }
    }
}

/*!
    @brief Handle logic for singleshot mode

    This method assumes the controller is in singleshot mode
*/
void PMACFilterController::_process_singleshot_state() {
    // Complete if singleshot run has stablised
    if (this->state_ == ControlState::ACTIVE && (
            this->last_received_frame_ >= this->last_processed_frame_ + 2 ||
            this->current_attenuation_ == 0
        )
    ) {
        std::cout << "Attenuation stabilised at " << this->current_attenuation_ << std::endl;
        this->_transition_state(ControlState::SINGLESHOT_COMPLETE);
        this->singleshot_start_ = false;
    }
    // Start singleshot run
    else if (this->singleshot_start_ && (
        this->state_ == ControlState::SINGLESHOT_WAITING ||
        this->state_ == ControlState::SINGLESHOT_COMPLETE
        )
    ) {
        // Set max attenuation and trigger the next run
        this->_transition_state(ControlState::WAITING);
        std::cout << "Starting a new singleshot run" << std::endl;
    }
}

/*!
    @brief Transition to the given state applying relevant logic for specific transitions
*/
void PMACFilterController::_transition_state(ControlState state) {
    if (state != this->state_) {
        bool waiting = state == ControlState::WAITING || state == ControlState::SINGLESHOT_WAITING;
        if (state < 1 || (waiting && this->state_ >= 0)) {
            this->_set_attenuation(MAX_ATTENUATION);
        }
    }

    this->state_ = state;
}

/*!
    @brief Process message and publish the resulting attenuation change

    @param[in] data_message Message containing histogram data
*/
void PMACFilterController::_handle_data_message(zmq::message_t& data_message) {
    std::string data_str = std::string(static_cast<char*>(data_message.data()), data_message.size());
    std::cout << "Data received: " << data_str << std::endl;
    json data = _parse_json_string(data_str);
    if (data.is_null()) {
        std::cout << "Not processing null data message" << std::endl;
        return;
    }

    int frame_number = data[FRAME_NUMBER];
    this->last_received_frame_ = frame_number;
    _get_time(&this->last_message_ts_);

    // Publish event with the adjustment made from the previous frame and the resulting current attenuation
    // This represents the state during the exposure of this frame
    this->_publish_event(frame_number);

    if (this->_process_data(data)) {
        this->last_processed_frame_ = frame_number;
        this->process_period_ = _useconds_since(this->last_process_ts_);
        _get_time(&this->last_process_ts_);
    } else {
        // Record that no adjustment was made for the most recent frame
        this->last_adjustment_ = 0;
    }
}

/*!
    @brief Process message data

    Determine if the `data` json should be processed based on the frame number, check which threshold is triggered if
    any and adjust the attenuation level as necessary.

    If PARAM_HIGH3 is triggered it will be processed regardless of the frame number.

    @param[in] data json structure of data message

    @return true if an attenuation change was made, else false
*/
bool PMACFilterController::_process_data(const json& data) {
    // Validate the data message
    if (!(data.contains(FRAME_NUMBER) && data.contains(PARAMETERS))) {
        std::cout << "Ignoring message - Does not have keys " << FRAME_NUMBER << " and " << PARAMETERS << std::endl;
        return false;
    }

    json histogram = data[PARAMETERS];

    // Close shutter if PARAM_HIGH3 threshold exceeded
    if (histogram[PARAM_HIGH3] > this->pixel_count_thresholds_[PARAM_HIGH3]) {
        this->_close_shutter();

        this->_trigger_threshold(PARAM_HIGH3);

        this->_transition_state(ControlState::HIGH3_TRIGGERED);
        std::cout << "Threshold " << PARAM_HIGH3 << " triggered - closing shutter" << std::endl;

        return true;
    }
    // Possibly ignore if PARAM_HIGH3 OK
    else if (data[FRAME_NUMBER] <= this->last_processed_frame_) {
        std::cout << "Ignoring message " << " - Already processed " << this->last_processed_frame_ << std::endl;
        return false;
    } else if (data[FRAME_NUMBER] == this->last_processed_frame_ + 1) {
        std::cout << "Ignoring message - Processed preceding frame" << std::endl;
        return false;
    }

    // Process logic for the most appropriate threshold
    // - Too many counts above high thresholds -> increase attenuation
    if (histogram[PARAM_HIGH2] > this->pixel_count_thresholds_[PARAM_HIGH2]) {
        this->_trigger_threshold(PARAM_HIGH2);
    } else if (histogram[PARAM_HIGH1] > this->pixel_count_thresholds_[PARAM_HIGH1]) {
        this->_trigger_threshold(PARAM_HIGH1);
    }
    // - Too few counts above low thresholds -> decrease attenuation
    else if (histogram[PARAM_LOW2] < this->pixel_count_thresholds_[PARAM_LOW2]) {
        this->_trigger_threshold(PARAM_LOW2);
    } else if (histogram[PARAM_LOW1] < this->pixel_count_thresholds_[PARAM_LOW1]) {
        this->_trigger_threshold(PARAM_LOW1);
    } else {
        return false;
    }

    return true;
}

/*!
    @brief Close shutter
*/
void PMACFilterController::_close_shutter() {
    
    char* temp = strdup(this->shutter_close_string_.c_str());

#ifdef __ARM_ARCH
        CommandTS(temp);
#endif

    free(temp);

}

/*!
    @brief Process filter adjustment

    @param[in] threshold Name of threshold to trigger
*/
void PMACFilterController::_trigger_threshold(const std::string threshold) {
    std::cout << threshold << " threshold triggered" << std::endl;

    int adjustment = THRESHOLD_ADJUSTMENTS.at(threshold);
    this->_set_attenuation(this->current_attenuation_ + adjustment);
    this->last_adjustment_ = adjustment;
}

/*!
    @brief Set updated attenuation demand on the motion controller

    Calculate positions of individual filters based on a bitmask of the attenuation level, set the parameters on the
    motion controller and then execute the motion program to move the motors.

    The code to set variables through shared memory is inside of an ARM ifdef fence, so when compiled for x86 it will
    just do the calculations and print a message.

    @param[in] attenuation Attenuation level to change to [0,15]
*/
void PMACFilterController::_set_attenuation(int attenuation) {
    if (attenuation <= 0) {
        std::cout << "Min attenuation reached" << std::endl;
        attenuation = 0;
    } else if (attenuation >= MAX_ATTENUATION) {
        std::cout << "Max attenuation reached" << std::endl;
        attenuation = MAX_ATTENUATION;
    }

    for (int idx = 0; idx < FILTER_COUNT; ++idx) {
        // Bit shift to get IN/OUT state of each filter
        this->final_demand_[idx] = (attenuation >> idx) & 1;
        // Prevent moving filters OUT in first move - if demand is OUT but current is IN, then stay IN until final move
        this->post_in_demand_[idx] = this->final_demand_[idx] | this->current_demand_[idx];
    }

#ifdef __ARM_ARCH
    std::cout << "Changing attenuation: " << this->current_attenuation_ << " -> " << attenuation << std::endl;

    // Set demands on ppmac (P407{1,2,3,4} and P408{1,2,3,4})
    for (int idx = 0; idx < FILTER_COUNT; ++idx) {
        // ppmac position = IN position if demand == 1 else OUT position
        pshm->P[4071 + idx] = this->post_in_demand_[idx] ? this->in_positions_[idx] : this->out_positions_[idx];
        pshm->P[4081 + idx] = this->final_demand_[idx] ? this->in_positions_[idx] : this->out_positions_[idx];
    }

    // Run the motion program
    CommandTS(RUN_PROG_1);

    this->move_duration_ = pshm->P[4085];
#else
    std::cout << "Not changing attenuation " << this->current_attenuation_ << " -> " << attenuation << std::endl;
#endif

    // Update current values for next incremental change
    for (int idx = 0; idx < FILTER_COUNT; ++idx) {
        this->current_demand_[idx] = this->final_demand_[idx];
    }
    this->current_attenuation_ = attenuation;
}

/*!
    @brief Publish an event with the given frame number, previous attenuation adjustment and current attenuation

    @param[in] frame_number Frame number of event
*/
void PMACFilterController::_publish_event(int frame_number) {
    json event = {
        {FRAME_NUMBER, frame_number},
        {ADJUSTMENT, this->last_adjustment_},
        {ATTENUATION, this->current_attenuation_}
    };

    std::string event_str = event.dump();
    zmq::message_t event_msg(event_str.size());
    memcpy(event_msg.data(), event_str.c_str(), event_str.size());
    this->zmq_publish_socket_.send(event_msg, 0);
}

/*!
    @brief Application entrypoint

    Validate command line arguments, create PMACFilterController and run.

    @return 0 when shutdown cleanly, 1 when given invalid arguments
*/
int main(int argc, char** argv) {
    if (argc != 4) {
        std::cout << "Usage: " << argv[0] << " control_port publish_endpoint subscribe_endpoints\n"
            << "e.g. '" << argv[0] << " 9000 9001 127.0.0.1:10009,127.0.0.1:10019'" << std::endl;

        if (argc == 2 && std::string(argv[1]) == "--help") {
            return 0;
        }
        return 1;
    }

    std::cout << "Version: " << VERSION << std::endl;

    std::string control_port(argv[1]);
    std::string publish_port(argv[2]);
    std::vector<std::string> subscribe_endpoints = _parse_endpoints(std::string(argv[3]));

    PMACFilterController pfc(control_port, publish_port, subscribe_endpoints);

#ifdef __ARM_ARCH
    InitLibrary();
#endif

    pfc.run();
    std::cout << "Finished run" << std::endl;

#ifdef __ARM_ARCH
    CloseLibrary();
#endif

    return 0;
}


/* Helper Methods */

/*!
    @brief Validate and parse json from a string representation to create a json object

    Note even if `parse(...)` does not throw an exception, the returned json object can be
    null (empty) and should be tested with `.is_null()` before access.

    @param[in] json_string String representation of a json structure

    @return json object parsed from string
*/
json _parse_json_string(const std::string& json_string) {
    json json;
    // Call json::accept first to determine if the string is valid json, without throwing an exception, before calling
    // json::parse, which does throw an exception for invalid json
    if (json::accept(json_string)) {
        json = json::parse(json_string);
    } else {
        std::cout << "Not valid JSON:\n" << json_string << std::endl;
    }

    return json;
}

/*!
    @brief Parse comma-separated string of endpoints from command line

    @param[in] endpoint_arg Comma-separated string of endpoints

    @return Vector of endpoints
*/
std::vector<std::string> _parse_endpoints(std::string endpoint_arg) {
    std::stringstream stream(endpoint_arg);

    std::vector<std::string> endpoint_vector;
    std::string endpoint;
    while (std::getline(stream, endpoint, ',')) {
        endpoint_vector.push_back("tcp://" + endpoint);
    }

    return endpoint_vector;
}

/*!
    @brief Check if a message is queued on the socket corresponding to the pollitem

    If this function returns true, then a `recv()` on the socket will return a message immediately.

    @param[in] pollitem `zmq_pollitem` corresponding to a `zmq_socket`

    @return true if there is a message queued on the socket, else false
*/
bool _message_queued(zmq::pollitem_t& pollitem) {
    return pollitem.revents & ZMQ_POLLIN;
}

/*!
    @brief Check if the given json is a valid request

    @param[in] request json object of request

    @return true if the request is valid, else false
*/

bool _is_valid_request(const json& request) {
    bool success = true;

    if (request.is_null()) {
        std::cout << "Failed to parse request as json" << std::endl;
        success = false;
    } else if (!request.contains(COMMAND)) {
        std::cout << "Request did not contain a '" << COMMAND << "' field" << std::endl;
        success = false;
    }

    return success;
}

/*!
    @brief Get the current time according to the system-wide realtime clock

    @param[out] ts Timespec to update with current time
*/

void _get_time(struct timespec* ts) {
    clock_gettime(CLOCK_REALTIME, ts);
}

/*!
    @brief Return the elapsed time since the given timespec in microseconds

    @param[in] start_ts Timespec of start time

    @return Microseconds since start time
*/

unsigned long _useconds_since(const struct timespec& start_ts) {
    struct timespec end_ts;
    long diff_s, diff_ns;
    clock_gettime(CLOCK_REALTIME, &end_ts);
    diff_ns = end_ts.tv_nsec - start_ts.tv_nsec;
    diff_s = end_ts.tv_sec - start_ts.tv_sec;

    long total_diff_us = (diff_s * 1000000) + (diff_ns / 1000);

    return total_diff_us;
}

/*!
    @brief Return the elapsed time since the given timespec in seconds

    The time is rounded down to the nearest second

    @param[in] start_ts Timespec of start time

    @return Seconds since start time
*/

unsigned long _seconds_since(const struct timespec& start_ts) {
    struct timespec end_ts;
    clock_gettime(CLOCK_REALTIME, &end_ts);

    return end_ts.tv_sec - start_ts.tv_sec;
}
