"""cmip6-mark-nullable

Revision ID: 6c698bb1c39f
Revises: 4dacd370801b
Create Date: 2025-04-12 13:46:28.606110

"""

from collections.abc import Sequence
from typing import Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "6c698bb1c39f"
down_revision: Union[str, None] = "4dacd370801b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("cmip6_dataset", schema=None) as batch_op:
        batch_op.alter_column("branch_method", existing_type=sa.VARCHAR(), nullable=True)
        batch_op.alter_column("branch_time_in_child", existing_type=sa.FLOAT(), nullable=True)
        batch_op.alter_column("branch_time_in_parent", existing_type=sa.FLOAT(), nullable=True)
        batch_op.alter_column("long_name", existing_type=sa.VARCHAR(), nullable=True)
        batch_op.alter_column("parent_activity_id", existing_type=sa.VARCHAR(), nullable=True)
        batch_op.alter_column("parent_experiment_id", existing_type=sa.VARCHAR(), nullable=True)
        batch_op.alter_column("parent_source_id", existing_type=sa.VARCHAR(), nullable=True)
        batch_op.alter_column("parent_time_units", existing_type=sa.VARCHAR(), nullable=True)
        batch_op.alter_column("parent_variant_label", existing_type=sa.VARCHAR(), nullable=True)
        batch_op.alter_column("vertical_levels", existing_type=sa.INTEGER(), nullable=True)
        batch_op.drop_column("init_year")


def downgrade() -> None:
    with op.batch_alter_table("cmip6_dataset", schema=None) as batch_op:
        batch_op.add_column(sa.Column("init_year", sa.INTEGER(), nullable=True))
        batch_op.alter_column("vertical_levels", existing_type=sa.INTEGER(), nullable=False)
        batch_op.alter_column("parent_variant_label", existing_type=sa.VARCHAR(), nullable=False)
        batch_op.alter_column("parent_time_units", existing_type=sa.VARCHAR(), nullable=False)
        batch_op.alter_column("parent_source_id", existing_type=sa.VARCHAR(), nullable=False)
        batch_op.alter_column("parent_experiment_id", existing_type=sa.VARCHAR(), nullable=False)
        batch_op.alter_column("parent_activity_id", existing_type=sa.VARCHAR(), nullable=False)
        batch_op.alter_column("long_name", existing_type=sa.VARCHAR(), nullable=False)
        batch_op.alter_column("branch_time_in_parent", existing_type=sa.FLOAT(), nullable=False)
        batch_op.alter_column("branch_time_in_child", existing_type=sa.FLOAT(), nullable=False)
        batch_op.alter_column("branch_method", existing_type=sa.VARCHAR(), nullable=False)
