#include "postgres.h"

#include "epoch/e_init.h"

#include "epoch/e_config.h"
#include "occ/o_config.h"

void EpochBootStrap(void)
{
    OccConfigRegisterGucs();
    EpochRegisterGucs();
}

void EpochModuleInit(void)
{
    EpochConfigInit();
}
