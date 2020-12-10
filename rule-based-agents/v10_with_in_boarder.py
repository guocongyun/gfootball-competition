from kaggle_environments.envs.football.helpers import *
import math
import random
import numpy as np

LONG_STEPS = 8.2
HIGH_STEPS = 6.9
SHORT_STEPS = 8.2
SHOT_STEPS = 7.5

RUN_SPEED = 0.015
PASS_SPEED = 0.025
SHOT_SPEED = 0.031
PASS_Y_SPEED = 0.265

ACCELERATION = 0.001

last_ball_owner = -1

def find_patterns(obs, player_x, player_y):
    """ find list of appropriate patterns in groups of memory patterns """
    for get_group in groups_of_memory_patterns:
        group = get_group(obs, player_x, player_y)
        if group["environment_fits"](obs, player_x, player_y):
            return group["get_memory_patterns"](obs, player_x, player_y)

def get_action_of_agent(obs, player_x, player_y):
    """ get action of appropriate pattern in agent's memory """
    memory_patterns = find_patterns(obs, player_x, player_y)
    # find appropriate pattern in list of memory patterns
    for get_pattern in memory_patterns:
        pattern = get_pattern(obs, player_x, player_y)
        if pattern["environment_fits"](obs, player_x, player_y):
            return pattern["get_action"](obs, player_x, player_y)

def is_within_border(obs, player_x, player_y, xborder = None, yborder = None, ball=False):
    if xborder: 
        distance_to_xborder = abs(xborder - player_x)
        if xborder*obs["left_team_direction"][obs["active"]][0] < 0: distance_to_xborder = 9999
        if xborder*player_x <= 0: distance_to_xborder = 9999
    else: distance_to_xborder = 9999
    if yborder: 
        distance_to_yborder = abs(yborder - player_y)
        if yborder*obs["left_team_direction"][obs["active"]][1] < 0: distance_to_yborder = 9999
        if yborder*player_y <= 0: distance_to_yborder = 9999
    else: distance_to_yborder = 9999

    if not ball: aceleration = ACCELERATION
    if ball: aceleration = 0.01
    if xborder and yborder: acceleration_ = math.sqrt(((aceleration)**2)/2)
    safe_steps_to_xborder = abs(obs["left_team_direction"][obs["active"]][0] / aceleration)
    safe_steps_to_yborder = abs(obs["left_team_direction"][obs["active"]][1] / aceleration)
    safe_distance_to_xborder = abs(obs["left_team_direction"][obs["active"]][0]*safe_steps_to_xborder - np.sign(obs["left_team_direction"][obs["active"]][0])*aceleration/2*(safe_steps_to_xborder)**2)
    safe_distance_to_yborder = abs(obs["left_team_direction"][obs["active"]][1]*safe_steps_to_yborder - np.sign(obs["left_team_direction"][obs["active"]][1])*aceleration/2*(safe_steps_to_yborder)**2)
    if distance_to_xborder < safe_distance_to_xborder + RUN_SPEED*2: 
        if xborder != None: return False
    if distance_to_yborder < safe_distance_to_yborder + RUN_SPEED*2: 
        if yborder != None: return False
    return True
        
def get_average_distance_to_opponents(obs, player_x, player_y):
    """ get average distance to closest opponents """
    distances_sum = 0
    distances_amount = 0
    for i in range(1, len(obs["right_team"])):
        # if opponent is ahead of player
        if obs["right_team"][i][0] > (player_x - 0.02):
            distance_to_opponent = get_distance(player_x, player_y, obs["right_team"][i][0], obs["right_team"][i][1])
            if distance_to_opponent < 0.03:
                distances_sum += distance_to_opponent
                distances_amount += 1
    # if there is no opponents close around
    if distances_amount == 0:
        return 2, distances_amount
    return distances_sum / distances_amount, distances_amount

def check_legal_move(obs, player_x, player_y):
    legal_move = False
    for i in range(1, len(obs["right_team"])):
        if obs["right_team"][i][0] > player_x + RUN_SPEED:
            legal_move = True
    if legal_move: return True
    else: return False

def check_steps(player_x, player_y, teammate_x, teammate_y, speed=PASS_SPEED):
    distance = normal_get_distance(player_x, player_y, teammate_x, teammate_y)
    steps = int(round(distance / speed))
    return steps

def is_blocked(obs, player_x, player_y, teammate_x, teammate_y, teammate_speed_x=0, teammate_speed_y=0, speed=PASS_SPEED, opponent_speed=1, high_pass=False):
    ball_pos = [player_x, player_y]
    if high_pass: 
        height_steps = int(round(1/PASS_Y_SPEED))
        steps = check_steps(player_x, player_y, teammate_x, teammate_y, speed)
        if steps == 0: return True
        if steps != 0: unit_direction = [(teammate_x - player_x)/steps, (teammate_y - player_y)/steps]
        ball_pos_ = ball_pos[:]
        for step in range(height_steps+1):
            ball_pos[0] += unit_direction[0]
            ball_pos[1] += unit_direction[1]
            for opponent in range(0, len(obs["right_team"])):
                distance_to_opponent = normal_get_distance(ball_pos[0], ball_pos[1], obs["right_team"][opponent][0]+step*obs["right_team_direction"][opponent][0], obs["right_team"][opponent][1]+step*obs["right_team_direction"][opponent][1])
                distance_to_teammate = normal_get_distance(ball_pos[0], ball_pos[1], teammate_x+teammate_speed_x*step, teammate_y+teammate_speed_y*step)
                if distance_to_opponent < RUN_SPEED*2:
                    return True
                if distance_to_opponent < distance_to_teammate + RUN_SPEED*2:
                    return True
        
        for step in range(steps-height_steps,steps+1):
            ball_pos[0] = ball_pos_[0] + step * unit_direction[0]
            ball_pos[1] = ball_pos_[1] + step * unit_direction[1]
            for opponent in range(0, len(obs["right_team"])):
                distance_to_opponent = normal_get_distance(ball_pos[0], ball_pos[1], obs["right_team"][opponent][0]+step*obs["right_team_direction"][opponent][0], obs["right_team"][opponent][1]+step*obs["right_team_direction"][opponent][1])
                distance_to_teammate = normal_get_distance(ball_pos[0], ball_pos[1], teammate_x+teammate_speed_x*step, teammate_y+teammate_speed_y*step)
                if distance_to_opponent < RUN_SPEED*2:
                    return True
                if distance_to_opponent < distance_to_teammate + RUN_SPEED*2:
                    return True

    if not high_pass:
        steps = check_steps(player_x, player_y, teammate_x, teammate_y, speed)
        if steps == 0: return True
        if steps != 0: unit_direction = [(teammate_x - player_x)/steps, (teammate_y - player_y)/steps]
        for step in range(steps+1):
            ball_pos[0] += unit_direction[0]
            ball_pos[1] += unit_direction[1]
            for opponent in range(0, len(obs["right_team"])):
                distance_to_opponent = normal_get_distance(ball_pos[0], ball_pos[1], obs["right_team"][opponent][0]+step*obs["right_team_direction"][opponent][0], obs["right_team"][opponent][1]+step*obs["right_team_direction"][opponent][1])
                distance_to_teammate = normal_get_distance(ball_pos[0], ball_pos[1], teammate_x+teammate_speed_x*step, teammate_y+teammate_speed_y*step)
                if distance_to_opponent < RUN_SPEED*2:
                    return True
                if distance_to_opponent < distance_to_teammate + RUN_SPEED*2:
                    return True
    return False

def normal_get_distance(x1, y1, x2, y2):
    """ get two-dimensional Euclidean distance, considering y size of the field """
    return math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)

def get_distance(x1, y1, x2, y2):
    """ get two-dimensional Euclidean distance, considering y size of the field """
    return math.sqrt((x1 - x2) ** 2 + (y1 * 2.38 - y2 * 2.38) ** 2)

def quadratic_equation_solver(a, b, c):
    sol1 = (-b+math.sqrt(b**2 - 4*a*c))/2/a
    sol2 = (-b-math.sqrt(b**2 - 4*a*c))/2/a
    return max(sol1, sol2)

def height_to_ball(obs, player_x, player_y):
    steps = quadratic_equation_solver(-0.0981/2, obs["ball_direction"][2], (obs["ball"][2] - 1.1))
    return steps

def bad_angle_short_pass(obs, player_x, player_y):
    """ perform a short pass, if player is at bad angle to opponent's goal """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # player is at bad angle to opponent's goal
        upper_bound = lambda x: x * 0.752 - 0.796
        lower_bound = lambda x: x * -0.752 + 0.796
        if player_y < upper_bound(player_x): return True
        elif player_y > lower_bound(player_x): return True
        else: return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        if player_y > 0:
            if Action.Top not in obs["sticky_actions"]:
                return Action.Top
        else:
            if Action.Bottom not in obs["sticky_actions"]:
                return Action.Bottom
        if Action.Sprint in obs["sticky_actions"]:
            return Action.ReleaseSprint
        if is_blocked(obs, player_x, player_y, 1, 0, SHOT_SPEED) and is_blocked(obs, player_x, player_y, 1, 0.044, SHOT_SPEED) and is_blocked(obs, player_x, player_y, 1, -0.044, SHOT_SPEED): return Action.HighPass
        else: return Action.LongPass

    return {"environment_fits": environment_fits, "get_action": get_action}

def close_to_goalkeeper_shot(obs, player_x, player_y):
    """ shot if close to the goalkeeper """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        goalkeeper_x = obs["right_team"][0][0] + obs["right_team_direction"][0][0] * 13
        goalkeeper_y = obs["right_team"][0][1] + obs["right_team_direction"][0][1] * 13
        # player located close to the goalkeeper
        if get_distance(player_x, player_y, goalkeeper_x, goalkeeper_y) < 0.3:
            return True
        if get_distance(player_x, player_y, 1, 0) < 0.1:
            return True
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        if player_y > 0 and player_y < 0.03:
            if Action.BottomRight not in obs["sticky_actions"]:
                return Action.BottomRight
        if player_y < 0 and player_y > -0.03:
            if Action.TopRight not in obs["sticky_actions"]:
                return Action.TopRight
        if Action.Sprint in obs["sticky_actions"]:
            return Action.ReleaseSprint
        return Action.Shot
    
    return {"environment_fits": environment_fits, "get_action": get_action}

def close_to_opponent_short_pass(obs, player_x, player_y):
    """ perform a short pass, if close to opponent's player and close to teammate """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        for i in range(1, len(obs["right_team"])):
            distance_to_opponent = get_distance(player_x, player_y, obs["right_team"][i][0], obs["right_team"][i][1])
            if distance_to_opponent < 0.03:
                for j in range(1, len(obs["left_team"])):
                    player_to_opponents = get_average_distance_to_opponents(obs, player_x, player_y)
                    teammate_to_opponents = get_average_distance_to_opponents(obs, obs["left_team"][j][0], obs["left_team"][j][1])
                    distance_to_teammate = get_distance(player_x, player_y, obs["left_team"][j][0], obs["left_team"][j][1])
                    if distance_to_teammate < 0.2 and player_to_opponents[0] < teammate_to_opponents[0] and check_legal_move(obs, player_x, player_y) and not is_blocked(obs, player_x, player_y, obs["left_team"][j][0], obs["left_team"][j][1]):
                        teammate_distance_to_goal = get_distance(obs["left_team"][j][0], obs["left_team"][j][1], 1, 0)
                        player_distance_to_goal = get_distance(player_x, player_y, 1, 0)
                        if teammate_distance_to_goal < player_distance_to_goal:
                            return True
                break
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        if Action.Sprint in obs["sticky_actions"]:
            return Action.ReleaseSprint
        return Action.ShortPass

    return {"environment_fits": environment_fits, "get_action": get_action}

def close_to_opponent_long_pass(obs, player_x, player_y):
    """ perform a long pass, if close to opponent's player and close to teammate """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        for i in range(1, len(obs["right_team"])):
            distance_to_opponent = get_distance(player_x, player_y, obs["right_team"][i][0], obs["right_team"][i][1])
            if distance_to_opponent < 0.06 and distance_to_opponent > 0.03:
                for j in range(1, len(obs["left_team"])):
                    player_to_opponents = get_average_distance_to_opponents(obs, player_x, player_y)
                    teammate_to_opponents = get_average_distance_to_opponents(obs, obs["left_team"][j][0], obs["left_team"][j][1])
                    distance_to_teammate = get_distance(player_x, player_y, obs["left_team"][j][0], obs["left_team"][j][1])
                    if distance_to_teammate < 0.4 and distance_to_teammate > 0.2 and player_to_opponents[0] < teammate_to_opponents[0] and check_legal_move(obs, player_x, player_y) and not is_blocked(obs, player_x, player_y, obs["left_team"][j][0], obs["left_team"][j][1]):
                        teammate_distance_to_goal = get_distance(obs["left_team"][j][0], obs["left_team"][j][1], 1, 0)
                        player_distance_to_goal = get_distance(player_x, player_y, 1, 0)
                        if teammate_distance_to_goal < player_distance_to_goal - 1/3:
                            return True
                break
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        if Action.Sprint in obs["sticky_actions"]:
            return Action.ReleaseSprint
        return Action.LongPass

    return {"environment_fits": environment_fits, "get_action": get_action}

def close_to_opponent_high_pass(obs, player_x, player_y):
    """ perform a high pass, if close to opponent's player and close to teammate """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        for i in range(1, len(obs["right_team"])):
            distance_to_opponent = get_distance(player_x, player_y, obs["right_team"][i][0], obs["right_team"][i][1])
            if distance_to_opponent > 0.06 and distance_to_opponent < 0.09:
                for j in range(1, len(obs["left_team"])):
                    player_to_opponents = get_average_distance_to_opponents(obs, player_x, player_y)
                    teammate_to_opponents = get_average_distance_to_opponents(obs, obs["left_team"][j][0], obs["left_team"][j][1])
                    distance_to_teammate = get_distance(player_x, player_y, obs["left_team"][j][0], obs["left_team"][j][1])
                    if distance_to_teammate < 0.6 and distance_to_teammate > 0.4 and player_to_opponents[0] < teammate_to_opponents[0] and check_legal_move(obs, player_x, player_y) and not is_blocked(obs, player_x, player_y, obs["left_team"][j][0], obs["left_team"][j][1], high_pass=True):
                        teammate_distance_to_goal = get_distance(obs["left_team"][j][0], obs["left_team"][j][1], 1, 0)
                        player_distance_to_goal = get_distance(player_x, player_y, 1, 0)
                        if teammate_distance_to_goal < player_distance_to_goal - 2/3:
                            return True
                break
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        if Action.Sprint in obs["sticky_actions"]:
            return Action.ReleaseSprint
        return Action.HighPass

    return {"environment_fits": environment_fits, "get_action": get_action}

def far_from_goal_shot(obs, player_x, player_y):
    """ perform a shot, if far from opponent's goal """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # player is far from opponent's goal or it's the goalkeeper
        if player_x < -0.6 or obs["ball_owned_player"] == 0:
            return True
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        if Action.Sprint in obs["sticky_actions"]:
            return Action.ReleaseSprint
        return Action.Shot
    
    return {"environment_fits": environment_fits, "get_action": get_action}

def far_from_goal_high_pass(obs, player_x, player_y):
    """ perform a high pass, if far from opponent's goal """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # player is far from opponent's goal or it's the goalkeeper
        if obs["ball_owned_player"] == 0:
            return True
        if get_distance(player_x, player_y, -1, 0) < 0.25 and obs["ball_owned_team"] == 0:
            return True
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        if Action.Right not in obs["sticky_actions"]:
            return Action.Right
        if Action.Sprint in obs["sticky_actions"]:
            return Action.ReleaseSprint
        return Action.HighPass
    
    return {"environment_fits": environment_fits, "get_action": get_action}

def go_through_opponents(obs, player_x, player_y):
    """ avoid closest opponents by going around them """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # right direction is safest
        biggest_distance, final_opponents_amount = get_average_distance_to_opponents(obs, player_x + 0.01, player_y)
        obs["memory_patterns"]["go_around_opponent"] = Action.Right
        # if top right direction is safest
        top_right, opponents_amount = get_average_distance_to_opponents(obs, player_x + 0.01, player_y - 0.01)
        if (top_right > biggest_distance and player_y > -0.15) or (top_right == 2 and player_y > 0.07):
            biggest_distance = top_right
            final_opponents_amount = opponents_amount
            obs["memory_patterns"]["go_around_opponent"] = Action.TopRight
        # if bottom right direction is safest
        bottom_right, opponents_amount = get_average_distance_to_opponents(obs, player_x + 0.01, player_y + 0.01)
        if (bottom_right > biggest_distance and player_y < 0.15) or (bottom_right == 2 and player_y < -0.07):
            biggest_distance = bottom_right
            final_opponents_amount = opponents_amount
            obs["memory_patterns"]["go_around_opponent"] = Action.BottomRight
        # is player surrounded?
        if final_opponents_amount >= 3:
            obs["memory_patterns"]["go_around_opponent_surrounded"] = True
        else:
            obs["memory_patterns"]["go_around_opponent_surrounded"] = False

        if not is_within_border(obs, player_x + obs["left_team_direction"][obs["active"]][0]*4, 0, xborder=-1, yborder=None) or not is_within_border(obs, obs["ball"][0] + obs["ball_direction"][0]*4, 0, xborder=1, yborder=None): 
            if obs["left_team_direction"][obs["active"]][1] < 0: obs["memory_patterns"]["go_around_opponent"] = Action.TopRight
            if obs["left_team_direction"][obs["active"]][1] > 0: obs["memory_patterns"]["go_around_opponent"] = Action.BottomRight
        if not is_within_border(obs, player_x + obs["left_team_direction"][obs["active"]][0]*4, 0, xborder=1, yborder=None) or not is_within_border(obs, obs["ball"][0] + obs["ball_direction"][0]*4, 0, xborder=1, yborder=None):
            if obs["left_team_direction"][obs["active"]][1] < 0: obs["memory_patterns"]["go_around_opponent"] = Action.TopLeft
            if obs["left_team_direction"][obs["active"]][1] > 0: obs["memory_patterns"]["go_around_opponent"] = Action.BottomLeft
        if not is_within_border(obs, 0, player_y + obs["left_team_direction"][obs["active"]][1]*4, xborder=None, yborder=0.42) or not is_within_border(obs, 0, obs["ball"][1] + obs["ball_direction"][1]*4, xborder=None, yborder=0.42): 
            if obs["left_team_direction"][obs["active"]][0] < 0: obs["memory_patterns"]["go_around_opponent"] = Action.TopLeft
            if obs["left_team_direction"][obs["active"]][0] > 0: obs["memory_patterns"]["go_around_opponent"] = Action.TopRight
        if not is_within_border(obs, 0, player_y + obs["left_team_direction"][obs["active"]][1]*4, xborder=None, yborder=-0.42) or not is_within_border(obs, 0, obs["ball"][1] + obs["ball_direction"][1]*4, xborder=None, yborder=-0.42): 
            if obs["left_team_direction"][obs["active"]][0] < 0: obs["memory_patterns"]["go_around_opponent"] = Action.BottomLeft
            if obs["left_team_direction"][obs["active"]][0] > 0: obs["memory_patterns"]["go_around_opponent"] = Action.BottomRight


        if not is_within_border(obs, player_x, 0, xborder=-1, yborder=None) or not is_within_border(obs, obs["ball"][0], 0, xborder=1, yborder=None, ball=True): obs["memory_patterns"]["go_around_opponent"] = Action.HighPass
        if not is_within_border(obs, player_x, 0, xborder=1, yborder=None) or not is_within_border(obs, obs["ball"][0], 0, xborder=1, yborder=None, ball=True): obs["memory_patterns"]["go_around_opponent"] = Action.HighPass
        if not is_within_border(obs, 0, player_y, xborder=None, yborder=0.42) or not is_within_border(obs, 0, obs["ball"][1], xborder=None, yborder=0.42, ball=True): obs["memory_patterns"]["go_around_opponent"] = Action.HighPass
        if not is_within_border(obs, 0, player_y, xborder=None, yborder=-0.42) or not is_within_border(obs, 0, obs["ball"][1], xborder=None, yborder=-0.42, ball=True): obs["memory_patterns"]["go_around_opponent"] = Action.HighPass

        return True
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        # if player is surrounded
        if obs["memory_patterns"]["go_around_opponent_surrounded"]:
            return Action.HighPass
        if Action.Sprint not in obs["sticky_actions"]:
            distance_to_opponent = [999]
            for i in range(1, len(obs["right_team"])):
                distance_to_opponent.append(get_distance(player_x, player_y, obs["right_team"][i][0], obs["right_team"][i][1]))
            min_distance_to_opponent = min(distance_to_opponent)
            if min_distance_to_opponent > RUN_SPEED: return Action.Sprint
        return obs["memory_patterns"]["go_around_opponent"]
    
    return {"environment_fits": environment_fits, "get_action": get_action}
    
def khorne_slide(obs, berzerker_x, berzerker_y):
    """ BLOOD FOR THE BLOOD GOD!!! SKULLS FOR THE SKULL THRONE!!! """
    def environment_fits(obs, berzerker_x, berzerker_y):
        """ environment fits constraints """
        # if prey has the ball
        if obs["left_team_yellow_card"][obs["active"]] != 1:
            if obs["ball_owned_team"] == 1:
                prey_x = obs["right_team"][obs["ball_owned_player"]][0]
                prey_y = obs["right_team"][obs["ball_owned_player"]][1]
                # by x position, amount of berzerker's team players between prey and goal of berzerker's team
                players_amount = 0
                nearby_players = 0
                for i in range(1, len(obs["left_team"])):
                    if obs["left_team"][i][0] < prey_x:
                        players_amount += 1
                    if get_distance(obs["left_team"][i][0], obs["left_team"][i][1], prey_x, prey_y) < RUN_SPEED*10: nearby_players += 1
                prey_x_direction = obs["right_team_direction"][obs["ball_owned_player"]][0]
                future_prey_x = prey_x + obs["right_team_direction"][obs["ball_owned_player"]][0]
                future_prey_y = prey_y + obs["right_team_direction"][obs["ball_owned_player"]][1]
                future_berzerker_x = berzerker_x + obs["left_team_direction"][obs["active"]][0]
                future_berzerker_y = berzerker_y + obs["left_team_direction"][obs["active"]][1]
                distance_to_prey = get_distance(berzerker_x, berzerker_y, prey_x, prey_y)
                future_distance_to_prey = get_distance(future_berzerker_x, future_berzerker_y, future_prey_x, future_prey_y)
                # if berzerker is not close to his own penalty zone
                # and prey is beyond x position of too many players of berzerker's team
                # and berzerker is close enough to prey
                # and berzerker is running in direction of prey
                if ((berzerker_x > -0.65 or abs(berzerker_y) > 0.3) and
                        players_amount <= 7 and
                        nearby_players != 1 and
                        nearby_players < 3 and
                        future_distance_to_prey < 0.015 and
                        distance_to_prey > future_distance_to_prey):
                    return True
            return False
        
    def get_action(obs, berzerker_x, berzerker_y):
        """ get action of this memory pattern """
        return Action.Slide
    
    return {"environment_fits": environment_fits, "get_action": get_action}

def run_to_ball_bottom(obs, player_x, player_y):
    """ run to the ball if it is to the bottom from player's position """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # ball is to the bottom from player's position
        if (obs["ball"][1] > player_y and
                abs(obs["ball"][0] - player_x) < 0.01):
            return True
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        return Action.Bottom
    
    return {"environment_fits": environment_fits, "get_action": get_action}

def run_to_ball_bottom_left(obs, player_x, player_y):
    """ run to the ball if it is to the bottom left from player's position """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # ball is to the bottom left from player's position
        if (obs["ball"][0] < player_x and
                obs["ball"][1] > player_y):
            return True
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        return Action.BottomLeft
    
    return {"environment_fits": environment_fits, "get_action": get_action}

def run_to_ball_bottom_right(obs, player_x, player_y):
    """ run to the ball if it is to the bottom right from player's position """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # ball is to the bottom right from player's position
        if (obs["ball"][0] > player_x and
                obs["ball"][1] > player_y):
            return True
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        return Action.BottomRight
    
    return {"environment_fits": environment_fits, "get_action": get_action}

def run_to_ball_left(obs, player_x, player_y):
    """ run to the ball if it is to the left from player's position """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # ball is to the left from player's position
        if (obs["ball"][0] < player_x and
                abs(obs["ball"][1] - player_y) < 0.01):
            return True
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        return Action.Left
    
    return {"environment_fits": environment_fits, "get_action": get_action}

def run_to_ball_right(obs, player_x, player_y):
    """ run to the ball if it is to the right from player's position """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # ball is to the right from player's position
        if (obs["ball"][0] > player_x and
                abs(obs["ball"][1] - player_y) < 0.01):
            return True
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        return Action.Right
    
    return {"environment_fits": environment_fits, "get_action": get_action}

def run_to_ball_top(obs, player_x, player_y):
    """ run to the ball if it is to the top from player's position """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # ball is to the top from player's position
        if (obs["ball"][1] < player_y and
                abs(obs["ball"][0] - player_x) < 0.01):
            return True
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        return Action.Top
    
    return {"environment_fits": environment_fits, "get_action": get_action}

def run_to_ball_top_left(obs, player_x, player_y):
    """ run to the ball if it is to the top left from player's position """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # ball is to the top left from player's position
        if (obs["ball"][0] < player_x and
                obs["ball"][1] < player_y):
            return True
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        return Action.TopLeft
    
    return {"environment_fits": environment_fits, "get_action": get_action}

def run_to_ball_top_right(obs, player_x, player_y):
    """ run to the ball if it is to the top right from player's position """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # ball is to the top right from player's position
        if (obs["ball"][0] > player_x and
                obs["ball"][1] < player_y):
            return True
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        return Action.TopRight
    
    return {"environment_fits": environment_fits, "get_action": get_action}

def idle(obs, player_x, player_y):
    """ do nothing, release all sticky actions """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        return True
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        return Action.Idle
    
    return {"environment_fits": environment_fits, "get_action": get_action}

def start_sprinting(obs, player_x, player_y):
    """ make sure player is sprinting """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        if Action.Sprint not in obs["sticky_actions"]:
            return True
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        return Action.Sprint
    
    return {"environment_fits": environment_fits, "get_action": get_action}
# "%%writefile -a submission.py" will append the code below to submission.py,
# it WILL NOT rewrite submission.py

def corner(obs, player_x, player_y):
    """ perform a shot in corner game mode """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # it is corner game mode
        if obs['game_mode'] == GameMode.Corner:
            return True
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        if player_y > 0:
            if Action.TopRight not in obs["sticky_actions"]:
                return Action.TopRight
        else:
            if Action.BottomRight not in obs["sticky_actions"]:
                return Action.BottomRight
        return Action.HighPass # check if it's not blocked 
    
    return {"environment_fits": environment_fits, "get_action": get_action}

def free_kick(obs, player_x, player_y):
    """ perform a high pass or a shot in free kick game mode """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # it is free kick game mode
        if obs['game_mode'] == GameMode.FreeKick:
            return True
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        # shot if player close to goal
        if player_x > 0.75:
            if player_y > 0:
                if Action.TopRight not in obs["sticky_actions"]:
                    return Action.TopRight
            else:
                if Action.BottomRight not in obs["sticky_actions"]:
                    return Action.BottomRight
            return Action.Shot
        # high pass if player far from goal
        else:
            if Action.Right not in obs["sticky_actions"]:
                return Action.Right
            return Action.HighPass
    
    return {"environment_fits": environment_fits, "get_action": get_action}

def goal_kick(obs, player_x, player_y):
    """ perform a short pass in goal kick game mode """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # it is goal kick game mode
        if obs['game_mode'] == GameMode.GoalKick:
            return True
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        if (random.random() < 0.5 and
                Action.TopRight not in obs["sticky_actions"] and
                Action.BottomRight not in obs["sticky_actions"]):
            return Action.TopRight
        else:
            if Action.BottomRight not in obs["sticky_actions"]:
                return Action.BottomRight
        return Action.ShortPass
    
    return {"environment_fits": environment_fits, "get_action": get_action}

def kick_off(obs, player_x, player_y):
    """ perform a short pass in kick off game mode """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # it is kick off game mode
        if obs['game_mode'] == GameMode.KickOff:
            return True
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        if player_y > 0:
            if Action.Top not in obs["sticky_actions"]:
                return Action.Top
        else:
            if Action.Bottom not in obs["sticky_actions"]:
                return Action.Bottom
        return Action.ShortPass
    
    return {"environment_fits": environment_fits, "get_action": get_action}

def penalty(obs, player_x, player_y):
    """ perform a shot in penalty game mode """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # it is penalty game mode
        if obs['game_mode'] == GameMode.Penalty:
            return True
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        if (random.random() < 0.5 and
                Action.TopRight not in obs["sticky_actions"] and
                Action.BottomRight not in obs["sticky_actions"]):
            return Action.TopRight
        else:
            if Action.BottomRight not in obs["sticky_actions"]:
                return Action.BottomRight
        return Action.Shot
    
    return {"environment_fits": environment_fits, "get_action": get_action}

def throw_in(obs, player_x, player_y):
    """ perform a short pass in throw in game mode """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # it is throw in game mode
        if obs['game_mode'] == GameMode.ThrowIn:
            return True
        return False
        
    def get_action(obs, player_x, player_y):
        """ get action of this memory pattern """
        if Action.Right not in obs["sticky_actions"]:
            return Action.Right
        return Action.ShortPass
    
    return {"environment_fits": environment_fits, "get_action": get_action}
# "%%writefile -a submission.py" will append the code below to submission.py,
# it WILL NOT rewrite submission.py

def defence_memory_patterns(obs, player_x, player_y):
    """ group of memory patterns for environments in which opponent's team has the ball """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # player don't have the ball
        if obs["ball_owned_team"] != 0:
            return True
        return False
        
    def get_memory_patterns(obs, player_x, player_y):
        """ get list of memory patterns """
        # shift ball position
        if obs["ball"][2] > 1.1 and obs["ball_owned_team"] == -1:
            steps = height_to_ball(obs, player_x, player_y)
            obs["ball"][0] += obs["ball_direction"][0] * steps
            obs["ball"][1] += obs["ball_direction"][1] * steps

        else:
            obs["ball"][0] += obs["ball_direction"][0] * 10/2.84*2
            obs["ball"][1] += obs["ball_direction"][1] * 10/2.84*0.84
            # if opponent has the ball and is far from y axis center
            if abs(obs["ball"][1]) > 0.07 and obs["ball_owned_team"] == 1:
                obs["ball"][0] -= 0.01
                if obs["ball"][1] > 0:
                    obs["ball"][1] -= 0.01
                else:
                    obs["ball"][1] += 0.01
            
        memory_patterns = [
            start_sprinting,
            khorne_slide,
            run_to_ball_right,
            run_to_ball_left,
            run_to_ball_bottom,
            run_to_ball_top,
            run_to_ball_top_right,
            run_to_ball_top_left,
            run_to_ball_bottom_right,
            run_to_ball_bottom_left,
            idle
        ]
        return memory_patterns
        
    return {"environment_fits": environment_fits, "get_memory_patterns": get_memory_patterns}

def goalkeeper_memory_patterns(obs, player_x, player_y):
    """ group of memory patterns for goalkeeper """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # player is a goalkeeper have the ball
        opponent_close = True
        if obs["ball_owned_player"] == obs["active"] and obs["ball_owned_player"] == 0:
            for step in range(10):
                player_distance = get_distance(obs["ball"][0]+step*obs["ball_direction"][0], obs["ball"][1]+step*obs["ball_direction"][1], obs["left_team"][0][0]+step*obs["left_team_direction"][0][0], obs["left_team"][0][1]+step*obs["left_team_direction"][0][1])
                if player_distance < RUN_SPEED*2:
                    opponent_close = False
                    for i in range(0, len(obs["right_team"])):
                        distance_to_opponent = get_distance(obs["ball"][0]+step*obs["ball_direction"][0], obs["ball"][1]+step*obs["ball_direction"][1], obs["right_team"][i][0]+step*obs["right_team_direction"][i][0], obs["right_team"][i][1]+step*obs["right_team_direction"][i][1])
                        if distance_to_opponent < player_distance*3: opponent_close = True
                if opponent_close: return False
            if not opponent_close: return True
        if (obs["ball_owned_player"] == obs["active"] and
                obs["ball_owned_team"] == 0 and
                obs["ball_owned_player"] == 0):
            return True
        return False

    def get_memory_patterns(obs, player_x, player_y):
        """ get list of memory patterns """
        memory_patterns = [
            close_to_opponent_short_pass,
            close_to_opponent_long_pass,
            close_to_opponent_high_pass,
            far_from_goal_high_pass,
            idle
        ]
        return memory_patterns
        
    return {"environment_fits": environment_fits, "get_memory_patterns": get_memory_patterns}

def offence_memory_patterns(obs, player_x, player_y):
    """ group of memory patterns for environments in which player's team has the ball """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # player have the ball
        if obs["ball_owned_player"] == obs["active"] and obs["ball_owned_team"] == 0:
            return True
        opponent_close = True
        for step in range(10):
            player_distance = get_distance(obs["ball"][0]+step*obs["ball_direction"][0], obs["ball"][1]+step*obs["ball_direction"][1], obs["left_team"][obs["active"]][0]+step*obs["left_team_direction"][obs["active"]][0], obs["left_team"][obs["active"]][1]+step*obs["left_team_direction"][obs["active"]][1])
            if player_distance < RUN_SPEED*2:
                opponent_close = False
                for i in range(0, len(obs["right_team"])):
                    distance_to_opponent = get_distance(obs["ball"][0]+step*obs["ball_direction"][0], obs["ball"][1]+step*obs["ball_direction"][1], obs["right_team"][i][0]+step*obs["right_team_direction"][i][0], obs["right_team"][i][1]+step*obs["right_team_direction"][i][1])
                    if distance_to_opponent < player_distance*2: opponent_close = True
            if opponent_close: return False
        if not opponent_close: return True
        return False

    def get_memory_patterns(obs, player_x, player_y):
        """ get list of memory patterns """
        memory_patterns = [
            far_from_goal_high_pass,
            bad_angle_short_pass,
            close_to_goalkeeper_shot,
            close_to_opponent_short_pass,
            close_to_opponent_long_pass,
            close_to_opponent_high_pass,
            go_through_opponents,
            idle
        ]
        return memory_patterns


    return {"environment_fits": environment_fits, "get_memory_patterns": get_memory_patterns}

def other_memory_patterns(obs, player_x, player_y):
    """ group of memory patterns for all other environments """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        return True
        
    def get_memory_patterns(obs, player_x, player_y):
        """ get list of memory patterns """
        memory_patterns = [
            idle
        ]
        return memory_patterns
        
    return {"environment_fits": environment_fits, "get_memory_patterns": get_memory_patterns}

def special_game_modes_memory_patterns(obs, player_x, player_y):
    """ group of memory patterns for special game mode environments """
    def environment_fits(obs, player_x, player_y):
        """ environment fits constraints """
        # if game mode is not normal
        if obs['game_mode'] != GameMode.Normal:
            return True
        return False
        
    def get_memory_patterns(obs, player_x, player_y):
        """ get list of memory patterns """
        memory_patterns = [
            corner,
            free_kick,
            goal_kick,
            kick_off,
            penalty,
            throw_in,
            idle
        ]
        return memory_patterns
        
    return {"environment_fits": environment_fits, "get_memory_patterns": get_memory_patterns}
# "%%writefile -a submission.py" will append the code below to submission.py,
# it WILL NOT rewrite submission.py

# list of groups of memory patterns
groups_of_memory_patterns = [
    special_game_modes_memory_patterns,
    goalkeeper_memory_patterns,
    offence_memory_patterns,
    defence_memory_patterns,
    other_memory_patterns
]

@human_readable_agent
def agent(obs):
    """ Ole ole ole ole """
    # dictionary for Memory Patterns data
    obs["memory_patterns"] = {}
    # We always control left team (observations and actions
    # are mirrored appropriately by the environment).
    controlled_player_pos = obs["left_team"][obs["active"]]
    global last_ball_owner
    if obs["ball_owned_team"] != last_ball_owner and obs["ball_owned_team"] != -1: last_ball_owner = obs["ball_owned_team"]
    # get action of appropriate pattern in agent's memory
    action = get_action_of_agent(obs, controlled_player_pos[0], controlled_player_pos[1])
    # return action
    return action
