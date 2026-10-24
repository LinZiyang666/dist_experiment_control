"""Admit the complete native producer graph before publishing a live supply.

Schema completeness cannot establish that a deployment export exists. It can
prevent discovering an omitted export after original business has started.
These checks do no I/O beyond reading pinned local executable/source bytes.
origin: hot-update S9 plan §9.1 / §9.4; S9 INSTRUMENTS-4.
"""
import infra_composer_contract as contract
import infra_owner_plan as plans
import hot_update_infra_scope as scope

NETWORK = ('nats-lost-quorum', 'nats-stream-lag', 'nats-consumer-lag')
LANES = ('writer_lost_ack', 'reader_partial_sync', 'commit_rename', 'terminal_courier')
EXERCISES = {'store-double-open': ('second-launch', 'legacy-owner'),
             'exact-release-join': ('mismatched-release', 'exact-release'),
             'caddy-responsibility-gates': scope.GATES}
CONFIG_ROOT = '/var/lib/tether-infra/configs'


def executable(value, port, reason):
    plans.need(type(value) is dict and 'argv' in value, port, reason)
    try:
        contract.spec(value)
    except (ValueError, OSError, KeyError, TypeError):
        plans.need(False, port, reason)


def validate(config, port):
    participants = config.get('participants')
    plans.need(type(participants) is dict and bool(participants), port, 'public_participant_export_required')
    for row in participants.values():
        plans.need(type(row) is dict and set(row) == {'node', 'ingress_node', 'reader'},
                   port, 'participant_source_fields')
        if type(row['reader']) is dict and 'custody' in row['reader']:
            from infra_native_exports import declaration
            declaration(row['reader'])
        else:
            executable(row['reader'], port, 'public_participant_export_required')
    for key, reason in (('repair_fence', 'p18_pre_repair_reader_required'),
                        ('phase_reader', 'physical_phase_export_required')):
        if key == 'repair_fence' and type(config.get(key)) is dict and 'custody' in config[key]:
            from infra_native_exports import declaration
            declaration(config[key], repair=True)
        else:
            executable(config.get(key), port, reason)
    executable(config.get('controls', {}).get('final'), port, 's5_raw_final_and_control_instrument_required')
    lanes = config.get('phase_lanes', {})
    plans.need(type(lanes) is dict and set(lanes) == set(LANES), port, 's7_phase_lane_instruments_required')
    for row in lanes.values():
        executable(row, port, 's7_phase_lane_instruments_required')
    # A DROP rule is only an injector. Every fault needs an independent hit
    # producer, including N2 whose topology must be read rather than renamed.
    faults = config.get('faults', {})
    plans.need(type(faults) is dict and set(faults) == set(contract.verbs.ALL_FAULTS), port, 'complete_faults_required')
    for point, actions in faults.items():
        plans.need(type(actions) is dict and set(actions) <= {'install', 'status', 'heal'}, port, 'fault_recipe_fields')
        for action in ('install', 'status', 'heal'):
            if action not in actions and point in NETWORK and action != 'status':
                continue  # The existing fault library owns these two mechanisms.
            executable(actions.get(action), port, 'fault_' + action + '_instrument_required_' + point)
    exercises = config.get('exercises', {})
    plans.need(type(exercises) is dict and set(exercises) == set(EXERCISES), port, 'complete_exercises_required')
    for name, labels in EXERCISES.items():
        recipes = exercises[name]
        plans.need(type(recipes) is dict and set(recipes) == set(labels), port, 'exercise_recipes_required_' + name)
        for recipe in recipes.values():
            plans.need(type(recipe) is dict and {'command', 'cleanup'} <= set(recipe) <=
                       {'setup', 'command', 'cleanup'}, port, 'exercise_recipe_fields')
            for row in recipe.values():
                executable(row, port, 'exercise_executable_required_' + name)
    executable(config.get('config_stage'), port, 'p26_config_stage_hook_required')
    # The root launcher opens this exact catalog. An instrument-owned copy,
    # even with identical bytes, cannot establish product selection custody.
    plans.need(config.get('config_root') == CONFIG_ROOT, port, 'product_config_catalog_required')
    return config
